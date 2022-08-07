import boto3
import os
import h5py as h5
import numpy as np


class S3gprDataFileIO:
    bucket = 'gpr-scan-data'

    def __init__(self, access_key='AKIA2UYKQHP63X5PQCMP',
                 secret_key='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', tmp_dir=os.path.join(os.getcwd(),
                                                                                             '../examples/tmp')):  # constructor
        self.aws_access_key = access_key
        self.aws_secret_key = secret_key
        self._session_open = False
        self._scan_id = None
        self._s3 = None
        self._files_in_folder = dict()
        self._files_in_folder['raw']=[]
        self._files_in_folder['img'] = []
        self._tmp_dir=tmp_dir

    def open_client_session(self, scan_id):
        if not self._session_open:
            self._session_open = True
            self._scan_id = scan_id
            self._s3 = boto3.client('s3',
                                    aws_access_key_id=self.aws_access_key,
                                    aws_secret_access_key=self.aws_secret_key)

    def close_client_session(self):
        if self._session_open:
            self._session_open = False
            self._s3.close()

    def list_files_in_folder(self, folder='raw'):
        """
        # lists all files in the raw folder within given scan folder
        """
        if not self._session_open:
            raise Exception('S3 client session is not open')
        response = self._s3.list_objects_v2(Bucket=self.bucket, Prefix=f'{self._scan_id}/{folder}/')
        self._files_in_folder[folder] = []

        for file_object in response.get('Contents', []):
            filename = file_object['Key'].split('/')[-1]
            if '.hdf5' in filename or 'png' in filename:
                self._files_in_folder[folder].append(filename)
        return self._files_in_folder[folder]

    def download_files(self, file_list=None, folder='raw'):  # download for internal use
        if not self._session_open:
            raise Exception('S3 client session is not open')
        if file_list is None:
            file_list = self._files_in_folder[folder]
        if type(file_list) == str:
            file_list = [file_list]
        local_dir = os.path.join(f'{self._tmp_dir}/{self._scan_id}/{folder}')
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        for filename in file_list:
            s3_file_path = os.path.join(self._scan_id, folder, filename)
            local_file_path = os.path.join(local_dir, filename)
            with open(local_file_path,'wb') as f:
                self._s3.download_fileobj(self.bucket, s3_file_path, f)

    def get_data(self, filename=None, folder='raw'):
        """
        download a raw data file given the scan folder, line number, and sweep number
        """
        self.download_files(file_list=filename,folder=folder)  # call download method
        h5_file = h5.File(os.path.join(self._tmp_dir,f'{self._scan_id}/{folder}/{filename}'),'r')  # get this file
        dataset = h5_file['sweep_data_raw']
        meta_data = {}  # get all attributes
        for key, value in dataset.attrs.items():
            meta_data[key] = value
        data = np.array(h5_file['/sweep_data_raw'])  # convert dataset to ndarray after pulling attributes
        return data, meta_data
        # returns dataset ndArray and a dictionary of its attributes in a list

    def combine_raw_hdf5(self, filename="all_raw.hdf5"):
        """
            # writes all raw data files to one larger htf5 file
        """
        #Need to check all files have been downloaded.
        path_filename = f'{self._tmp_dir}/{self._scan_id}/raw/{filename}'
        if os.path.exists(path_filename):
            os.remove(path_filename)
        hdf5_file = h5.File(path_filename, 'w')
        meta_data_attr = None

        for scan_file in self._files_in_folder['raw']:
            line, profile = scan_file.split('.')[0].split('-')
            data, meta_data = self.get_data(filename=scan_file, folder='raw')
            if not meta_data_attr:
                meta_data_attr = meta_data
            hdf5_file.create_dataset(f'{profile}/line-{line}', data=data)

        for key in meta_data_attr:
            hdf5_file.attrs[key] = meta_data_attr[key]

        hdf5_file.close()

