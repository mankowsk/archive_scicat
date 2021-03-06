from ctypes import addressof
import json
from logging import exception
import pathlib
from tkinter import W
import numpy as np
import requests
import subprocess
from pathlib import Path
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import os
import pandas as pd
import elog
from getpass import getpass
dacat = 'https://dacat.psi.ch/api/v3/'
leo_ingestor = 'https://sf-ingestor-2.psi.ch/api/v1/ingestion/'

class ScicatClient():
    def __init__(
        self,
        user = None,
        tokenfile = "/sf/bernina/config/scicat/tokens",
    ):
        self.tokenfile = tokenfile
        self.token = None
        if user == None:
            user = input("user name for ingestion: ")
        self.user = user
        if not self.get_token():
            self.set_token()
        print(self.user, self.token)

    def get_token(self):
        try:
            with open(self.tokenfile, "r") as f:
                tokens = f.read()
        except:
            return False
        tokens = np.array(tokens.split("\n"))
        idx = [self.user in line for line in tokens]
        if np.any(idx):
            self.token = tokens[idx][0].split(" ")[1]
            print(f"Loaded token for user {self.user}: {self.token}")
            return True
        else:
            return False

    def set_token(self):
        token = input("token not found. please login to https://discovery.psi.ch and paste Catamel Token of your user: ")
        token = token.strip(" ")
        self.token = token
        try:
            with open(self.tokenfile, "a") as f:
                f.writelines([f"\n{self.user} {token}"])
            print(f"Saved token for user {self.user}: {self.token}")
        except Exception as e:
            print(f"Saving of token failed, token only available in memory: {e}")

def read_json_1(file_name_json):
    p = pathlib.Path(file_name_json)
    assert p.is_file(), "Input string does not describe a valid file path."
    with p.open(mode="r") as f:
        s = json.load(f)
    assert len(s["scan_files"]) == len(
        s["scan_values"]
    ), "number of files and scan values don't match in {}".format(file_name_json)
    assert len(s["scan_files"]) == len(
        s["scan_readbacks"]
    ), "number of files and scan readbacks don't match in {}".format(file_name_json)
    return s, p

def read_json_2(file_name_json):
    p = pathlib.Path(file_name_json)
    with p.open(mode="r") as f:
        s = json.load(f)
    return s, p
read_json = read_json_2

def convert_jsons(pgroup = None, current_json_directory = None, new_directory = None, path_to_replace=None, replace_by='../'):

    if path_to_replace is None:
        path_to_replace = f'/sf/bernina/data/{pgroup}/'
    if current_json_directory is None:
        current_json_directory = Path(f'/sf/bernina/data/{pgroup}/res/scan_info/')
    if new_directory is None:
        new_directory = Path(f'/das/work/{pgroup[:3]}/{pgroup}/scan_info/')
    if not new_directory.exists():
        new_directory.mkdir(parents=True)
    runs = {int(f.stem[3:7]): f for f in current_json_directory.glob('*.json')}
    print(f'Original json directory: {current_json_directory}')
    print(f'Modified json directory: {new_directory}')
    print(f"Found {len(runs)} json files")
    exceptions = {}
    for run, f in runs.items():
        try:
            print(f"converting run {run}")
            s,p=read_json(f)
            scan_files = s['scan_files']
            scan_files_new = [[step_file.replace(path_to_replace, replace_by) for step_file in step_files] for step_files in scan_files]
            s['scan_files'] = scan_files_new
            new_json_file = new_directory.joinpath(p.name)
            with new_json_file.open('w') as f:
                json.dump(s, f)
        except Exception as e:
            exceptions[run] = str(e)
    if len(exceptions) > 0:
        print('DONE WITH THE FOLLOWING EXCEPTIONS:')
        for run, e in exceptions.items():
            print(run, e)
    else:
        print("DONE WITHOUT EXCEPTIONS")

class Data_Catalogue_Class():
    def __init__(
        self,
        name = None,
        address = None,
        pid = None,
        client = None,
        **kwargs,
        ):
        self.name = name
        self.client = client
        self.pid = pid
        self.address = address


    def get(self, data=None):
        return requests.get(dacat+Path(f'datasets/{self.pid.replace("/","%2F")}/{self.address}/?access_token={self.client.token}').as_posix(), json=data, verify = False).json()

    def __call__(self, data=None):
        return self.get()

class Data_Catalogue_Item(Data_Catalogue_Class):
    def __init__(
        self,
        name = None,
        address = None,
        pid = None,
        client = None,
        ds = None,
        dcl = None,
    ):
        super().__init__(
            name = name,
            address = address,
            pid = pid,
            client = client,
        )
        self._items = {}
        self.ds = ds
        self.dcl = dcl

    def delete(self):
        data = {'ownerGroup': self.ds()['ownerGroup']}
        res = requests.delete(dacat+Path(f'datasets/{self.pid.replace("/","%2F")}/{self.address}/?access_token={self.client.token}').as_posix(), json=data, verify = False)
        if res.ok:
            p = self.dcl._items.pop(self.name)
            p = self.dcl.__dict__.pop(self.name)
        return res

    def _update(self, **kwargs):
        data = {'ownerGroup': self.ds()['ownerGroup']}
        data.update(kwargs)
        res = requests.put(dacat+Path(f'datasets/{self.pid.replace("/","%2F")}/{self.address}/?access_token={self.client.token}').as_posix(), json=data, verify = False)
        return res

class Data_Catalogue_List(Data_Catalogue_Class):
    def __init__(
        self,
        name = None,
        address = None,
        pid = None,
        client = None,
        ds = None,
        Item_Class = Data_Catalogue_Item
    ):
        self._Item_Class = Item_Class
        super().__init__(
            name = name,
            address = address,
            pid = pid,
            client = client,
        )
        self._items = {}
        self.ds = ds
        self.init_items()

    def init_items(self):
        try:
            self._items = {}
            for item in self.get():
                n = len(self._items)
                name = f'{self.name}_{n}'
                iid = item['id']
                if iid in self._items.values():
                    continue
                self.__dict__[name] = self._Item_Class(name = name, address=f'{self.address}/{iid}', pid=self.pid, client=self.client, ds=self.ds, dcl=self)
                self._items[name] = iid
        except Exception as e:
            print(f'initialization of {self.name} items failed')
            print(e)

    def _post(self, **kwargs):
        data = {'ownerGroup': self.ds()['ownerGroup']}
        data.update(kwargs)
        res = requests.post(dacat+Path(f'datasets/{self.pid.replace("/","%2F")}/{self.address}/?access_token={self.client.token}').as_posix(), json=data, verify = False)
        if res.ok:
            n = len(self._items)
            name = f'{self.name}_{n}'
            iid = res.json()['id']
            self.__dict__[name] = self._Item_Class(name = name, address=f'{self.address}/{iid}', pid=self.pid, client=self.client, ds=self.ds, dcl=self)
            self._items[name] = iid
        return res

    def delete_all(self):
        res = requests.delete(dacat+Path(f'datasets/{self.pid.replace("/","%2F")}/{self.address}/?access_token={self.client.token}').as_posix(), verify = False)
        if res.ok:
            for key in self._items.keys():
                try:
                    p = self.__dict__.pop(key)
                except:
                    continue
            self._items = {}
        return res

    def __iter__(self):
        for attr in self._items.keys():
            yield self.__dict__[attr]

class OrigFiles(Data_Catalogue_List):
    def __init__(self, **kwargs):
        super().__init__(Item_Class = Data_Catalogue_Item, **kwargs)
    def create(self, files_list, caption=''):
        data = [{'path': file}for file in files_list]
        size = float(np.sum([Path(file).stat().st_size for file in files_list]))
        return self._post(dataFileList=data, size=size)

class Attachments(Data_Catalogue_List):
    def __init__(self, **kwargs):
        super().__init__(Item_Class = Attachment_Item, **kwargs)
    def create(self, image_path, caption=''):
        return self._post(thumbnail=image_path, caption=caption)

class Attachment_Item(Data_Catalogue_Item):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
    def update(self, image_path, caption=''):
        return self._update(thumbnail=image_path, caption=caption)

class HistoryList(Data_Catalogue_List):
    def __init__(self, **kwargs):
        super().__init__(Item_Class = Data_Catalogue_Item, **kwargs)
    def create(self):
        print('Cannot post in history')
        return

class Samples(Data_Catalogue_List):
    def __init__(self, **kwargs):
        super().__init__(Item_Class = Sample_Item, **kwargs)
    def create(self, sample_id='', description=None, sample_characteristics=None):
        return self._post(sampleId=sample_id, description=description, sampleCharacteristics=sample_characteristics)

class Sample_Item(Data_Catalogue_Item):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    def update(self, sample_id='', description=None, sample_characteristics=None):
        return self._update(sampleId=sample_id, description=description, sampleCharacteristics=sample_characteristics)

class TechniquesList(Data_Catalogue_List):
    def __init__(self, **kwargs):
        super().__init__(Item_Class = Technique_item, **kwargs)
    def create(self, technique_name=''):
        return self._post(name=technique_name)

class Technique_item(Data_Catalogue_Item):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    def update(self, technique_name=''):
        return self._update(name=technique_name)


class Dataset(Data_Catalogue_Class):
    def __init__(
        self,
        link = None,
        pid = None,
        name = None,
        client = None,
        pgroup = None,
        run_number = None,
        get_datacat_metadata = True
        ):
        if client is None:
            client = ScicatClient()
        self.pgroup = pgroup
        self.run_number = run_number
        super().__init__(
            name = name,
            address = '',
            pid= pid,
            client = client,
        )
        if get_datacat_metadata:
            if self.pid is None:
                if self.pgroup is not None and self.name is not None:
                    self.pid = self.get_pid_from_name(dataset_name=self.name)
            if self.pid is not None:
                data = self.get()
                self.pgroup = data['ownerGroup']
                self.name = data['datasetName']
                self.attachments = Attachments(
                    name = 'attachment',
                    address = 'attachments/',
                    pid = self.pid,
                    client = self.client,
                    ds=self,
                )
                self.files_original = OrigFiles(
                    name = 'files',
                    address = 'origdatablocks/',
                    pid = self.pid,
                    client = self.client,
                    ds=self,
                )
                self.history = HistoryList(
                    name = 'event',
                    address = 'historyList/',
                    pid = self.pid,
                    client = self.client,
                    ds=self,
                )
                self.techniques = TechniquesList(
                    name = 'technique',
                    address = 'techniquesList/',
                    pid = self.pid,
                    client = self.client,
                    ds=self,
                )
                self.samples = Samples(
                    name = 'technique',
                    address = 'techniquesList/',
                    pid = self.pid,
                    client = self.client,
                    ds=self,
                )
                self.scientific_metadata = Data_Catalogue_Class(
                    name = 'scientific_metadata',
                    address = '/',
                    pid = self.pid,
                    client = self.client,
                    ds=self,
                )
                self.link = f"https://discovery.psi.ch/datasets/{self.pid.replace('/', '%2F')}"
        self.archive_data = Archiver(ds=self)

    def update(self, dat={}, **kwargs):
        data = {}
        data.update(dat)
        data.update({'ownerGroup': self()['ownerGroup']})
        data.update(kwargs)
        for key in 'ingest auto_archive requester_user metadata attachments'.split(' '):
            if key in data.keys():
                p = data.pop(key)
        res = requests.put(dacat+Path(f'datasets/{self.pid.replace("/","%2F")}/?access_token={self.client.token}').as_posix(), json=data, verify = False)
        return res

    def delete(self):
        data = {'ownerGroup': self()['ownerGroup']}
        res = requests.delete(dacat+Path(f'datasets/{self.pid.replace("/","%2F")}/').as_posix(), json=data, headers={"Authorization": "Token 917222f9ea5134219e9314ddd0a35333c1e5576a"}, verify = False)
        return res

    def get_pid_from_name(self, dataset_name=None, pgroup = None):
        pid = None
        if pgroup is None:
            pgroup = self.pgroup
        if dataset_name is None:
            dataset_name = self.name
        filter = {
            'ownerGroup': pgroup,
            'datasetName': dataset_name,
            }
        res = self.get_filtered(filter=filter)
        if res.ok:
            if len(res.json())==1:
                pid = res.json()[0]['pid']
        return pid

    def get_filtered(self, filter):
        data = {
            'fields': filter
        }

        res = requests.get(f'{dacat}/datasets/fullquery/?access_token={self.client.token}', json=data, verify=False)
        return res

class Datasets():
    def __init__(
        self
        ):
        pass

    def append_ds(self, ds, name):
        self.__dict__[name] = ds

    def __iter__(self):
        for attr in dir(self):
            if not np.any([attr.startswith("__"), attr == "append_ds"]):
                yield self.__dict__[attr]

class Experiment():
    def __init__(
        self,
        pgroup = None,
        elog_address = 'https://elog-gfa.psi.ch/Bernina',
    ):
        self.pgroup = pgroup
        self.rt = None
        self.client = ScicatClient()
        self.datasets = Datasets()
        self.datasets_from_scicat = Datasets()
        if elog_address is not None:
            self.log = elog.open(elog_address)

    def init_datasets_from_scicat(self, json_directory = None, converted_json_directory = None, run_table_keys=None, elog=False, overwrite=False):
        datasets = self.get_datacat_datasets_filtered(filter = {'ownerGroup': self.pgroup}).json()
        for dataset in datasets:
            run_number = None
            try:
                scimet = dataset['scientificMetadata']
                if 'runNumber' in scimet.keys():
                    if type(scimet['runNumber']) == dict:
                        run_number = scimet['runNumber']['value']
                    else:
                        run_number = scimet['runNumber']
            except:
                pass
            ds = Dataset(
                client=self.client,
                pgroup=self.pgroup,
                pid = dataset['pid'],
                name = dataset['datasetName'],
                run_number = run_number,
            )
            self.datasets_from_scicat.append_ds(name=dataset['datasetName'].replace(" ", "_"), ds=ds)
            print(f'Appended {dataset["datasetName"]} to self.datasets_from_scicat')

    def init_datasets_from_json_directory(self, json_directory = None, converted_json_directory = None, run_table_keys=None, elog=False):
        if type(json_directory) == str:
            json_directory = Path(json_directory)
        if type(converted_json_directory) == str:
            converted_json_directory = Path(converted_json_directory)
        if json_directory is None:
            json_directory = Path(f'/sf/bernina/data/{self.pgroup}/res/scan_info/')
        if json_directory is str:
            json_directory=Path(json_directory)
        if converted_json_directory is None:
            converted_json_directory = Path(f'/das/work/{self.pgroup[:3]}/{self.pgroup}/scan_info/')
        print(f'Converted Json File Directory: {converted_json_directory}')
        print(f'Original Json File Directory: {json_directory}')
        if not converted_json_directory.exists():
            converted_json_directory = None
            print('------DIRECTORY OF CONVERTED JSON FILES NOT FOUND: ONLY ORIGINAL JSON FILES WILL BE ARCHIVED------')
        assert(f'/sf/bernina/data/{self.pgroup}/res/' in json_directory.as_posix()), f'/sf/bernina/data/{self.pgroup}/res/ is not in the json directories - please use absolute paths for archiving to work'
        runs = {int(f.stem[3:7]): f for f in json_directory.glob('*.json')}
        self.runs = runs
        for run_number, json_file_path in runs.items():
            print(run_number)
            ds = Dataset(
                client=self.client,
                pgroup=self.pgroup,
                run_number=run_number,
                name=json_file_path.stem,
                pid=None
                )
            try:
                data = ds.archive_data.get_metadata_from_json(json_file_path=json_file_path.as_posix())
            except Exception as e:
                print(f'Retrieving json information failed for run {run_number}')
                print(e)
                continue
            if run_table_keys is not None:
                if self.rt is None:
                    self.rt = ds.archive_data.get_runtable(pgroup=self.pgroup)
                try:
                    data = ds.archive_data.get_metadata_from_runtable(keys=run_table_keys, rt=self.rt)
                except Exception as e:
                    print(f'Retrieving run_table information failed for run {run_number}')
                    print(e)
            if elog:
                try:
                    data = ds.archive_data.get_metadata_from_elog(elog_instance=self.log, rt = self.rt, run_number = run_number)
                except Exception as e:
                    print(f'Retrieving elog information failed for run {run_number}')
                    print(e)
            if converted_json_directory is not None:
                json_conv = converted_json_directory.joinpath(json_file_path.name)
                assert json_conv.exists(), f'Converted json file {json_conv} not found'
                ds.archive_data.data['files_list'].append(json_conv.as_posix())
            self.datasets.append_ds(name=f'run_{run_number:04}', ds=ds)

    def get_datacat_dataset_pid_from_name(self, dataset_name, pgroup = None):
        pid = None
        if pgroup is None:
            pgroup = self.pgroup
        filter = {
            'ownerGroup': pgroup,
            'datasetName': dataset_name,
            }
        res = self.get_datacat_datasets_filtered(filter=filter)
        if res.ok:
            if len(res.json())==1:
                pid = res.json()[0]['pid']
        return pid

    def get_datacat_datasets_filtered(self, filter):
        data = {
            'fields': filter
        }
        res = requests.get(f'{dacat}datasets/fullquery/?access_token={self.client.token}', json=data, verify=False)
        return res

    def get_dataset_files(self, pid):
        res = requests.get(f'{dacat}datasets/fullquery/?access_token={self.client.token}', json=data, verify=False)

    def check_for_non_archived_files(self, directory=None):
        """
        Returns a dictionary containing numpy arrays with the files found on SciCat, the local files in all 
        subfolders of the specified directory and the files, which are in the directory but not on scicat
        """
        pgroup = self.pgroup
        if directory == None:
            directory = f'/sf/bernina/data/{pgroup}/'
            print(f'Folder not specified, parsing all subfolders of /res/ and /raw/ of the pgroup directory: {directory}')
            local_path_res = Path(directory+'res/')
            local_files_res = np.array([f.as_posix() for f in local_path_res.rglob('*.*') if f.is_file()])
            print(f'Found {len(local_files_res)} files in {self.pgroup}/res/')

            local_path_raw = Path(directory+'raw/')
            local_files_raw = np.array([f.as_posix() for f in local_path_raw.rglob('*.*') if f.is_file()])
            print(f'Found {len(local_files_raw)} files in {self.pgroup}/raw/')

            local_files = np.hstack([local_files_res, local_files_raw])
        else:
            print(f'Parsing all subfolders of directory: {directory}')
            local_path = Path(directory)
            local_files = np.array([f.as_posix() for f in local_path.rglob('*.*') if f.is_file()])
            print(f'Found {len(local_files)} files in {self.pgroup}')



        data = {'fields': {'ownerGroup': pgroup}}
        js_scicat = requests.get(f'{dacat}/origdatablocks/fullquery/?access_token={self.client.token}', json=data, verify=False).json()
        sc_pids, sc_files = np.array([[f['datasetId'], f['dataFileList']['path']] for f in js_scicat]).T
        print(f'Found {len(js_scicat)} files on SciCat of pgroup {self.pgroup}')

        archived_ds = self.get_datacat_datasets_filtered({'ownerGroup': self.pgroup, 'datasetlifecycle.retrievable': True}).json()
        all_ds = self.get_datacat_datasets_filtered({'ownerGroup': self.pgroup, 'datasetlifecycle.retrievable': True}).json()
        all_ds_pids = np.array([ds['pid'] for ds in all_ds])
        archived_ds_pids = np.array([ds['pid'] for ds in archived_ds])
        print(f'Dataset stats: {len(archived_ds_pids)} out of {len(all_ds_pids)} datasets on SciCat are archived')
        sc_files_archived = sc_files[np.isin(sc_pids, archived_ds_pids)]
        print(f'File stats: {len(sc_files_archived)} out of {len(js_scicat)} files on SciCat are archived')

        data = {
            "scicat": sc_files,
            "scicat_archived": sc_files_archived,
            "local": local_files,
            "local_not_archived": local_files[~np.isin(local_files, sc_files_archived)],
            "scicat_datasets_not_archived": all_ds_pids[~np.isin(all_ds_pids, archived_ds_pids)]
        }
        return data

    def append_dataset_from_files(self, name, files):
        """Appends a dataset with a specified file list to the Experiment instance. 
        Adds only the most basic metadata required for archiving"""
        ds = Dataset(
        pid=None,
        name=name,
        client=self.client,
        pgroup=self.pgroup,
        run_number=None,
        get_datacat_metadata=True,
        )

        metadata = {
        'files_list': files,
        'metadata': {'type': 'raw',
        'creationLocation': '/PSI/swissfel/bernina',
        'sourceFolder': '//',
        'ownerGroup': self.pgroup,
        'datasetName': name},
        'scientificMetadata': {},
        }
        
        ds.archive_data.data=metadata
        self.datasets.append_ds(ds, name)
        print(f"Appended {name} to datasets")

    def append_dataset_from_directories(self, name, directories, recursive = True):
        """Searches files in the given list of top directories and their subfolders (recursive by default) and appends a dataset to the Experiment instance. 
        Adds only the most basic metadata required for archiving"""

        files = []
        for directory in directories:
            p = Path(directory)
            if recursive:
                f = [a.as_posix() for a in p.rglob("*.*")]
            else:
                f = [a.as_posix() for a in p.glob("*.*")]
            files.extend(f)
        self.append_dataset_from_files(name=name, files=files)

class Archiver:
    def __init__(
        self,
        ds = None
    ):
        self.ds=ds
        self.data={}

    def remove_leading_slash(self, files):
        fs = []
        for f in files:
            if f[0] == "/":
                fs.append(f[1:])
            else:
                fs.append(f)
        return fs

    def get_metadata_from_json(self, run_number  = None, pgroup=None, scan_info_dir = '/res/scan_info/', json_file_path=None):
        if pgroup is None:
            pgroup = self.ds.pgroup
        if run_number is None:
            run_number = self.ds.run_number
        if json_file_path is None:
            json_file_path = self.json_file_path_from_run_number(run_number=run_number, pgroup=pgroup, scan_info_dir=scan_info_dir)
        s, p =read_json(json_file_path)
        p = p.resolve()
        self.ds.name = p.stem
        files_json = np.hstack(s['scan_files'])
        files = [f for f in files_json if Path(f).is_file()]
        assert len(files) > 0, f'No files found in the json file {json_file_path}'
        files.append(json_file_path)
        data = {
            'requester_user': 'swissfelaramis-bernina',
            'ingest': True,
            'auto_archive': False,
            'files_list': files,
        }
        data['metadata'] = {
            'type': 'raw',
            'creationLocation': f'/PSI/{p.parts[3]}/{p.parts[5]}',
            'sourceFolder': str(Path(s['scan_files'][0][0]).parent),
            'ownerGroup': p.parts[6],
            'datasetName': p.stem,
        }
        meta_sci = {
            'runNumber': {'value': int(run_number), 'unit': ''},
            'scan_name': {'value':p.stem, 'unit': ''},
            }
        adjs = s['scan_parameters']
        for n, (adj, pv) in enumerate(zip(adjs['name'], adjs['Id'])):
            meta_sci[f'scan_adjustable{n}'] = {'value': adj, 'unit': ''}
            meta_sci[f'scan_adjustable{n}_id'] = {'value': pv, 'unit': ''}
        data = self.replace_dots(data)
        meta_sci = self.replace_dots(meta_sci)
        self.data.update(data)
        if not 'scientificMetadata' in self.data.keys():
            self.data['scientificMetadata'] = {}
        self.data['scientificMetadata'].update(meta_sci)
        return data

    def get_runtable(self, pgroup=None):
        if pgroup is None:
            pgroup = self.ds.pgroup
        rt_old = Path(f'/sf/bernina/data/{pgroup}/res/runtables/{pgroup}_adjustable_runtable.pkl')
        rt_new = Path(f'/sf/bernina/data/{pgroup}/res/run_table/{pgroup}_runtable.pkl')
        if rt_new.is_file():
            rt = pd.read_pickle(rt_new.as_posix())
        elif rt_old.is_file():
            rt = pd.read_pickle(rt_old.as_posix())
            i1 = rt.columns.get_level_values(0)
            i2 = rt.columns.get_level_values(1)
            newcols = pd.Index([f'{idx1}.{idx2}' for idx1, idx2 in zip(i1,i2)])
            newcols = pd.Index(['.'.join([s if not "_self" in s else "self" for s in f'{idx}'.split('.')]) for idx in newcols])
            rt.columns = newcols
        else:
            print('No runtable files found')
        rt = rt[~rt.index.duplicated(keep='last')]
        rt = rt.fillna('')
        return rt

    def get_metadata_from_runtable(self, run_number = None, pgroup=None, keys=None, rt=None):
        if rt is None:
            rt = self.get_runtable(pgroup=pgroup)
        if pgroup is None:
            pgroup = self.ds.pgroup
        if run_number is None:
            run_number = self.ds.run_number
        data = {}
        for key in keys:
            data[key] = {'value': rt.loc[run_number][key], 'unit': ''}
        if not 'scientificMetadata' in self.data.keys():
            self.data['scientificMetadata'] = {}
        data = self.replace_dots(data)
        self.data['scientificMetadata'].update(data)
        return data

    def get_metadata_from_elog(self, msg_id = None, elog_address = None, elog_instance=None, rt = None, run_number = None):
        if elog_instance is None:
            elog_instance = elog.open(elog_address)
        if msg_id is None:
            if rt is not None:
                if run_number is None:
                    run_number = self.ds.run_number
                msg_id = int(rt.loc[run_number]['metadata.elog_message_id'])
                msg_link = rt.loc[run_number]['metadata.elog_post_link']
        msg = elog_instance.read(msg_id)
        data={}
        data['attachments']=[{'thumbnail': hlink, 'caption': msg_link} for hlink in msg[-1]]
        data['description']=msg[0]
        data = self.replace_dots(data)
        self.data.update(data)
        return data

    def json_file_path_from_run_number(self, run_number, pgroup, scan_info_dir = '/res/scan_info/'):
        print(f'/sf/bernina/data/{pgroup}{scan_info_dir}')
        runs = os.listdir(f'/sf/bernina/data/{pgroup}{scan_info_dir}')
        name = [r for r in runs if f'{run_number:04}' in r][0]
        return f'/sf/bernina/data/{pgroup}{scan_info_dir}{name}'

    def append_attachments_from_data(self, clean=False):
        assert self.ds.pid is not None, "Dataset has no PID - it might not yet be ingested into the data catalogue"
        assert 'attachments' in self.data.keys(), "There are no attachments in self.data.keys()"
        attachments = self.data['attachments']
        for att in attachments:
            image_path = att['thumbnail']
            caption = att['caption']
            if clean:
                existing_atts = [ea for ea in self.ds.attachments]
                for existing_att in existing_atts:
                    if existing_att()['thumbnail'] == image_path:
                        existing_att.delete()
            self.ds.attachments.create(image_path=image_path, caption=caption)


    def ingest_leo(self, autoarchive=False, ingest=True):
        ingestor_api = leo_ingestor
        data = self.data
        data['ingest'] = ingest
        data['auto_archive'] = autoarchive
        res = requests.post(ingestor_api, json=data,headers={"Authorization": "Token 917222f9ea5134219e9314ddd0a35333c1e5576a"}, verify=False)
        return res

    def export_metadata(self, fname, data):
        with open(fname, 'w') as f:
            json.dump(data, f)

    def export_files_list(self, fname, data):
        np.savetxt(fname, data, fmt='%s')

    def ingest_from_Ra(self, folder_filelist = './archive', autoarchive=False, ingest=True, silent=False, test_env=False):

        data = self.data.copy()
        #data['files_list'] = self.remove_leading_slash(data['files_list'])
        data['metadata']['sourceFolder']="//"
        js = {}
        js.update(data['metadata'])
        if 'scientificMetadata' in data.keys():
            js['scientificMetadata'] = data['scientificMetadata']
        if 'description' in data.keys():
            js['description'] = data['description']
        p = Path(folder_filelist)
        if not p.is_dir():
            p.mkdir(mode = 766)
        self.export_metadata(f'{folder_filelist}/metadata_{self.ds.run_number}.json', js)
        self.export_files_list(f'{folder_filelist}/files_{self.ds.run_number}.txt', data['files_list'])
        s = ["/opt/psi/Tools/datacatalog/1.1.10/bin/datasetIngestor"]
        if ingest:
            s.append("-ingest")
        if autoarchive:
            s.append("-autoarchive")
        if test_env:
            s.append("-testenv")
        #user = input("user name for ingestion: ")
        #pw = getpass()
        #s.append(f"-user {user}:{pw}")
        s.append(f"-token {self.ds.client.token}")
        #-nocopy prevents using rsync to copy the data to psi
        s.append(f"-nocopy")
        if silent:
            s.append("-noninteractive")
        s.append("-allowexistingsource")
        s.append(f'{folder_filelist}/metadata_{self.ds.run_number}.json')
        s.append(f'{folder_filelist}/files_{self.ds.run_number}.txt')
        subprocess.run(" ".join(s), shell=True)
        return s

    def replace_dots(self, data):
        new = {}
        for k, v in data.items():
            if isinstance(v, dict):
                v = self.replace_dots(v)
            new[k.replace('.', '-')] = v
        return new



