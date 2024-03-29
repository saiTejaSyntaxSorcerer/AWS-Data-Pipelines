from paramiko import Transport, SFTPClient
import time
import errno
import logging
logging.basicConfig(format='%(levelname)s : %(message)s',
                    level=logging.INFO)


class SftpClient:

    _connection = None

    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        self.create_connection(self.host, self.port,
                               self.username, self.password)

    @classmethod
    def create_connection(cls, host, port, username, password):

        transport = Transport(sock=(host, port))
        transport.connect(username=username, password=password)
        cls._connection = SFTPClient.from_transport(transport)

    @staticmethod
    def uploading_info(uploaded_file_size, total_file_size):

        logging.info('uploaded_file_size : {} total_file_size : {}'.
                     format(uploaded_file_size, total_file_size))

    def upload(self, local_path, remote_path):

        self._connection.put(localpath=local_path,
                             remotepath=remote_path,
                             callback=self.uploading_info,
                             confirm=True)

    def file_exists(self, remote_path):

        try:
            print('remote path : ', remote_path)
            self._connection.stat(remote_path)
        except e:
            if e.errno == errno.ENOENT:
                return False
            raise
        else:
            return True
    def list_files(self, remote_path):
        latest_relevant_files =[]
        try:
            files_list = self._connection.listdir()
            print(files_list)
        except e:
            if e.errno == errno.ENOENT:
                return False
            raise
        for i in files_list:
            if '.dat.gz' in i:
                latest_relevant_files.append(i)
        print(latest_relevant_files)
        
        for i in latest_relevant_files:
            download_remote_path = '/'+i
            download_local_path =i
            client.download(download_remote_path, download_local_path)
            
        

    def download(self, remote_path, local_path, retry=5):

        if self.file_exists(remote_path) or retry == 0:
            self._connection.get(remote_path, local_path,
                                 callback=None)
        elif retry > 0:
            time.sleep(5)
            retry = retry - 1
            self.download(remote_path, local_path, retry=retry)
    def close(self):
        self._connection.close()
        
if __name__ == '__main__':

    host = ''
    port = 22
    username = ''
    password = ''
    client = SftpClient(host, port,username, password)

    #client.download(download_remote_path, download_local_path)
    download_remote_path = '/inbound/symphony'
    local_path = '//app/troy-prod/MM_PP_PLANXREF_20220531.dat.gz'
    client.upload(local_path,download_remote_path)
    client.close()