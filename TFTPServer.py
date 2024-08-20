import socket
import threading
import queue
import time

class TFTPServer:
    
    MAX_BLOCK_SIZE = 512
    
    TFTP_OPCODES = {     
        'RRQ': 1,
        'WRQ': 2,
        'DATA': 3,
        'ACK': 4,
        'ERROR': 5
    }
    
    def __init__(self, host, port, timeout=10):
        self.srv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.host = host
        self.port = port
        self.socketTimeout = timeout
        
        self.stop_event = threading.Event()
        
        self.clients = {}
        self.client_queues = {}             # Dict pour stocker les queues des clients
        self.lock = threading.Lock()        # VEROU accès conccurentiel
    
    def start(self):
        """Démarre le serveur, appelé dans un script principal"""
        self.srv_socket.bind((self.host, self.port))
        self.srv_socket.settimeout(self.socketTimeout)
        try:
            threading.Thread(target=self.__listener).start()
        except Exception as e:
            print(f"Erreur: {e}")
        
    def stop(self):
        """Stop le serveur, appelé dans un script principal"""
        self.stop_event.set()
        self.srv_socket.close()
    
    def __listener(self):
        while not self.stop_event.is_set():
            try:
                message, client_address = self.srv_socket.recvfrom(516)
                opCode = int.from_bytes(message[:2], 'big')

                if opCode == TFTPServer.TFTP_OPCODES['WRQ']:
                    with self.lock:
                        if client_address not in self.clients:
                            self.clients[client_address] = {'blockNumber': 0, 'filename': None, 'lastAck': None}
                            self.client_queues[client_address] = queue.Queue()
                    
                    wrqThread = threading.Thread(target=self.__handle_wrq, args=(client_address, message)) ####OPTIMISER
                    wrqThread.daemon = True
                    wrqThread.start()
            
                elif opCode == TFTPServer.TFTP_OPCODES['DATA']:
                    with self.lock:
                        if client_address in self.client_queues:
                            self.client_queues[client_address].put(message)
                    
            except socket.timeout:
                print("SOCKET Timeout")
                 
            except Exception as e:
                print(f"Erreur: {e}")
                break
 
    def __handle_wrq(self, client_address, message):
        parts = message[2:].split(b'\x00')
        filename = parts[0].decode()
        ackPacket = TFTPServer.create_ack_packet(0)
        
        with self.lock:
            self.clients[client_address]['filename'] = filename
            self.clients[client_address]['lastAck'] = ackPacket

        self.__send(ackPacket, client_address)
        with self.lock:
            self.clients[client_address]['blockNumber'] += 1

        # Lancer un worker dédié pour ce client
        worker_thread = threading.Thread(target=self.__wrqWorker, args=(client_address,))
        worker_thread.daemon = True
        worker_thread.start()
    
    def __wrqWorker(self, client_address):
        with self.lock:
            filename = self.clients[client_address]['filename']
            blockNumber = self.clients[client_address]['blockNumber']

        try:
            with open(filename, 'wb') as f:
                while True:
                    message = self.client_queues[client_address].get()
                    receivedBlockNumber = int.from_bytes(message[2:4], 'big')
                    data = message[4:]

                    #print(f"Reçu {receivedBlockNumber}, Attendu {blockNumber}")

                    if receivedBlockNumber == blockNumber:
                        f.write(data)
                        
                        ackPacket = TFTPServer.create_ack_packet(blockNumber)
                        self.__send(ackPacket, client_address)
                        blockNumber += 1

                        with self.lock:
                            self.clients[client_address]['lastAck'] = ackPacket
                            self.clients[client_address]['blockNumber'] = blockNumber

                        print(f"Envoyé ACK pour Block {blockNumber}")

                        if len(data) < TFTPServer.MAX_BLOCK_SIZE:  # Dernier paquet
                            print(f"Transfert terminé pour {client_address}")
                            break

                    elif receivedBlockNumber == blockNumber - 1:  # Paquet dupliqué
                        with self.lock:
                            print(f"Reçu Block dupliqué {receivedBlockNumber}, renvoie du dernier ACK")
                            self.__send(self.clients[client_address]['lastAck'], client_address)
                    else:
                        print(f"Erreur: Paquet DATA hors séquence. Reçu: Block {receivedBlockNumber}")

        finally:
            with self.lock:
                del self.clients[client_address]
                del self.client_queues[client_address]
            #print(f"KILL Worker {client_address}")

    def __send(self, dataToSend, clientSocket):
        """Envoie des données via la socket"""
        self.srv_socket.sendto(dataToSend, clientSocket)
        print("PAQUET ENVOYE: ", dataToSend)

    @staticmethod
    def create_ack_packet(block_number):
        """Crée un paquet ACK TFTP pour le numéro de bloc spécifié."""
        ack_packet = b'\x00\x04' + block_number.to_bytes(2, 'big')
        return ack_packet
         
if __name__ == "__main__":
    srv = TFTPServer('0.0.0.0', 69)
    srv.start()
    print("Serveur TFTP démarré")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        srv.stop()
        print("Serveur TFTP arrêté")