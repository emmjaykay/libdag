
import logging
import tempfile
from abc import ABCMeta, abstractmethod
import os

import numpy as np
import deepdish as dd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AbstractBaseNode(object):
    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(str(__class__))
        self.outputs = None

        # Extract the previous outputs and create them as inputs
        self.inputs = kwargs.get('input', None)

        self.api_key = kwargs['user_api_key']
        self.experiment_run = kwargs['experiment_run']
        self.inputs = kwargs['input_file']

    def unpack(self, *args, **kwargs):
        """
        Intake a dictionary of labels-to-paths and return a dictionary of labels
        to objects
        :param args:
        :param kwargs: a dictionary of labels to paths
        :return: dictionary of labels to objects
        """
        if self.inputs:
            if type(self.inputs) != list:
                data = [dd.io.load(path=self.inputs)]
            else:
                data = []
                for input in self.inputs:
                    s = dd.io.load(path=input)
                    data.append(s)
        else:
            return None

        return data

    def exe(self, *args, **kwargs):
        """
        This is the main code that will run the object code to be run
        :param args:
        :param kwargs: A dictionary of parameters
        :return: status of unpacking, running, and saving
        """

        # step 1 - unpack the previous
        unpacked_data = self.unpack()

        # step 2 - exec code and generate numpy objects to save
        self.outputs = self.run(data=unpacked_data)

        # step 3 - save code, create an H5 file from the objects
        result_filename = self.save()

        # Step 4 - return the path to the outside world somehow
        # For now, log an output path and the container.logs() will output it to be
        # returnd to another node
        # TODO: Create a callback system that will send back output paths to the DAG runner API
        print(result_filename)
        return result_filename

    @abstractmethod
    def run(self, data, *args, **kwargs):
        # Transform input data and return objects to save
        # The user defines this
        raise NotImplementedError("You need to implement this class")

    def save(self, *args, **kwargs):
        # Save the actual output data automatically
        # This will be not user defined
        # self.logger.info("Saving %s" % self.outputs)

        path_to_save = self._generate_save_dir_path()
        temp_file = tempfile.NamedTemporaryFile(dir=path_to_save, suffix='.h5', delete=False, prefix=self.__class__.__name__ + '-')

        dd.io.save(temp_file.name, self.outputs)
        # Save the outputs dictionary to a h5 file

        return temp_file.name

    def _generate_save_dir_path(self):
        s = ''
        data_dir = "/data"

        s = os.path.join(s, data_dir)
        s = os.path.join(s, self.api_key)
        s = os.path.join(s, self.experiment_run)

        try:
            os.mkdir(s)
        except OSError:
            # TODO: Look at logging this as an error of some kind in case we run something again when we shouldn't
            pass

        return s


# TODO: It's probably not that great that we have to manually unpack and name files

class ObjectA(AbstractBaseNode):
    def run(self, *args, **kwargs):
        data = np.ones(7)
        return {'objectA': data}


class ObjectB(AbstractBaseNode):
    def run(self, data, *args, **kwargs):
        objB = np.ones(7) + data[0]['objectA']
        return {'objectB': objB}


class ObjectC(AbstractBaseNode):
    def run(self, data, *args, **kwargs):
        objB = np.ones(7)
        for _data in data:
            for k in _data.keys():
                objB += _data[k]
        return {'objectC': objB}


class ObjectD(AbstractBaseNode):
    def run(self, data, *args, **kwargs):
        objB = np.ones(7)
        for _data in data:
            for k in _data.keys():
                objB += _data[k]
        return {'objectD': objB}

if __name__ == "__main__":
    user_api_key = 'mjk_api_key'
    experiment_run = 'aaaaa'
    inputs = ['/data/mjk_api_key/4e55d8f9-91f8-4b4e-ad29-26ee40de799e/ObjectB-5d8wq29f.h5',
              '/data/mjk_api_key/4e55d8f9-91f8-4b4e-ad29-26ee40de799e/ObjectC-4ikecv3_.h5'
              ]
    n = ObjectD(user_api_key=user_api_key, experiment_run=experiment_run, input_file=inputs).exe()

    i = 0