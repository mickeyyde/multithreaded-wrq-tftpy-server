# logging.info are in French

import socket
import threading
import queue
import logging
import os

from time import sleep
from datetime import datetime

class TFTPServer:
    SAVE_PATH = '../data/files/'
    MAX_BLOCK_SIZE = 512
    TFTP_OPCODES = {     
        1 : 'RRQ',
        2 : 'WRQ',
        3 : 'DATA',
        4 : 'ACK',
        5 : 'ERROR'
    }
    
    def __init__(self, host, port, timeout=10):
        self.srv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.host = host
        self.port = port
        self.socketTimeout = timeout
        self.full_path = None
        
        self.stop_event = threading.Event()
        
        self.clients = {}               # (x.x.x.x -> ipAddress, YYYY -> port): {'blockNumber': 0, 'filename': None, 'lastAck': None}
        self.client_queues = {}         # (x.x.x.x -> ipAddress, YYYY -> port): queue.Queue()
        
        self.lock = threading.Lock()    # since each client has its own thread, we need to lock the shared resources
    
    
    def start(self):
        """
        Start the server, should be called in the main script
        """
        self.srv_socket.bind((self.host, self.port))
        self.srv_socket.settimeout(self.socketTimeout)
        logging.debug(f"socket binded to host:{self.host} port:{self.port} timeout:{self.socketTimeout}")
        try:
            threading.Thread(target=self.__listener).start()
            logging.debug("__listener thread started")
            logging.info(f"Serveur TFTP démarré sur {self.host}:{self.port}")   
        except Exception as e:
            logging.error(f"{e}")
        
    def stop(self):
        """
        Stop the server, should be called in the main script
        """
        self.stop_event.set()
        self.srv_socket.close()
        logging.info(f"Serveur TFTP {self.host}:{self.port} arrêté")
    
    def __listener(self):
        """
        Listen for incoming requests and create a QUEUE for each CLIENT sending a WRQ request
        Add DATA packets to the CLIENT's QUEUE
        """
        while not self.stop_event.is_set():
            try:
                message, client_address = self.srv_socket.recvfrom(516)
                opCode = int.from_bytes(message[:2], 'big')
                logging.debug(f"received opcode {opCode}[{TFTPServer.TFTP_OPCODES[opCode]}] from {client_address}")
                
                ### -------------------------------------- WRQ ------------------------------------- ###
                if TFTPServer.TFTP_OPCODES[opCode] == 'WRQ':
                    if self.full_path is None:
                        self.full_path = self.__createDir()  
                    with self.lock:
                        if client_address not in self.clients:
                            self.clients[client_address] = {'blockNumber': 0, 'filename': None, 'lastAck': None}
                            self.client_queues[client_address] = queue.Queue()
                            logging.debug(f"client {client_address} added to clients")
                    
                    threading.Thread(target=self.__handle_wrq, args=(client_address, message),daemon=True).start()
                    logging.debug(f"__handle_wrq thread started for {client_address}")
                    
                ### ------------------------------------- DATA ------------------------------------- ###
                elif TFTPServer.TFTP_OPCODES[opCode] == 'DATA':
                    with self.lock:
                        if client_address in self.client_queues:
                            self.client_queues[client_address].put(message)
                                   
            except socket.timeout:
                logging.debug(f"socket timeout [socketTimeout={self.socketTimeout}]")
            except Exception as e:
                logging.error(f"{e}")
                logging.error(f"__listener BROKEN")
                break
 
    def __handle_wrq(self, client_address, message):
        """
        Handle WRQ requests, create a worker thread for each client to handle the file transfer
        """
        parts = message[2:].split(b'\x00')
        filename = parts[0].decode()
        ackPacket = TFTPServer.create_ack_packet(0)
        logging.info(f"{client_address}: Début du transfert du fichier {filename}")
    
        with self.lock:
            self.clients[client_address]['filename'] = filename
            self.clients[client_address]['lastAck'] = ackPacket
        self.__send(ackPacket, client_address)
        
        with self.lock:
            self.clients[client_address]['blockNumber'] += 1
        # Launch worker thread for the client
        worker_thread = threading.Thread(target=self.__wrqWorker, args=(client_address,), daemon=True).start()
        logging.debug(f"__wrqWorker thread started for {client_address} at {worker_thread}")
    
    def __wrqWorker(self, client_address):
        """
        Handle the file transfer, watch the CLIENT's queue
        """
        with self.lock:
            filename = self.clients[client_address]['filename']
            blockNumber = self.clients[client_address]['blockNumber']

        try:
            file_path = os.path.join(self.full_path, filename)
            with open(file_path, 'wb') as f:
                while True:
                    message = self.client_queues[client_address].get()
                    receivedBlockNumber = int.from_bytes(message[2:4], 'big')
                    data = message[4:]

                    if receivedBlockNumber == blockNumber:
                        f.write(data)
                        
                        ackPacket = TFTPServer.create_ack_packet(blockNumber)
                        self.__send(ackPacket, client_address)

                        blockNumber += 1

                        with self.lock:
                            self.clients[client_address]['lastAck'] = ackPacket
                            self.clients[client_address]['blockNumber'] = blockNumber

                        if len(data) < TFTPServer.MAX_BLOCK_SIZE:  # Dernier paquet
                            logging.debug(f"{client_address}: LAST PACKET, transfert finished")
                            break

                    elif receivedBlockNumber == blockNumber - 1:  # Paquet dupliqué
                        with self.lock:
                            logging.warning(f"{client_address}: DUPLICATED {receivedBlockNumber}, __send last ACK")
                            self.__send(self.clients[client_address]['lastAck'], client_address)
                    else:
                        logging.error(f"{client_address}: DATA OFF SEQUENCE, got: Block {receivedBlockNumber} wanted: Block {blockNumber}")
                        
        except Exception as e:
            logging.error(f"{client_address}: __wrqWORKER: {e}")
            
        finally:
            with self.lock:
                del self.clients[client_address]
                del self.client_queues[client_address]
                logging.info(f"{client_address}: Transfert terminé")
                logging.debug(f"{client_address}: __wrqWORKER finished")

    def __send(self, dataToSend, clientSocket):
        """
        Send packet to the client.
        """
        self.srv_socket.sendto(dataToSend, clientSocket)
        logging.debug(f"{clientSocket}: __send {dataToSend}")

    @staticmethod
    def create_ack_packet(block_number):
        """
        Create an ACK packet for specified block_number
        """
        ack_packet = b'\x00\x04' + block_number.to_bytes(2, 'big')
        return ack_packet
    
    def __createDir(self):
        """
        Quick method to create a directory with the current date and an incremental ID
        """
        date_str = datetime.now().strftime('%Y-%m-%d')
        id_num = 1
        while True:
            dir_name = f"{date_str}-{id_num:02d}"
            full_path = os.path.join(TFTPServer.SAVE_PATH, dir_name)
            if not os.path.exists(full_path):
                os.makedirs(full_path)
                logging.debug(f"__createDir: {full_path}")
                return os.path.join(full_path)
            id_num += 1
         
if __name__ == "__main__":
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logging.warning("you may need to disable local antivirus software")
    
    srv = TFTPServer('0.0.0.0', 69)
    srv.start()
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        srv.stop()