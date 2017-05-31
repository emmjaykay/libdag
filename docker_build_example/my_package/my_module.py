
import logging
from abc import ABCMeta, abstractmethod
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AbstractBaseNode(object):
    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(str(__class__))
        self.outputs = None
        self.inputs = None

        self.exe()

    def unpack(self, *args, **kwargs):
        """
        Intake a dictionary of labels-to-paths and return a dictionary of labels
        to objects
        :param args:
        :param kwargs: a dictionary of labels to paths
        :return: dictionary of labels to objects
        """

        pass

    def exe(self, *args, **kwargs):
        """
        This is the main code that will run the object code to be run
        :param args:
        :param kwargs: A dictionary of parameters
        :return: status of unpacking, running, and saving
        """
        # Extract the previous outputs and create them as inputs
        self.inputs = kwargs.get('old_outputs', None)

        # step 2 - exec code and generate numpy objects to save
        self.outputs = self.run(inputs=self.inputs)

        # step 3 - save code, create an H5 file from the objects
        self.save()

        # Step 4 - return the path to the outside world somehow
        # For now, log an output path and the container.logs() will output it to be
        # returnd to another node
        # TODO: Create a callback system that will send back output paths to the DAG runner API
        self.logger.info("Saved to Path such and such")

    @abstractmethod
    def run(self, *args, **kwargs):
        # Transform input data and return objects to save
        # The user defines this
        raise NotImplementedError("You need to implement this class")

    def save(self, *args, **kwargs):
        # Save the actual output data automatically
        # This will be not user defined
        self.logger.info("Saving %s" % self.outputs)

        # Code here to save numpy objects to save to H5
        return None # return a status


class ObjectA(AbstractBaseNode):
    def run(self, **kwargs):
        data = np.ones(7)
        self.logger.info("ObjectA running")

        return {'objectA': data}


class ObjectB(AbstractBaseNode):
    def run(self):
        self.logger.info("ObjectB running")


class ObjectC(AbstractBaseNode):
    def run(self):
        self.logger.info("ObjectC running")


class ObjectD(AbstractBaseNode):
    def run(self):
        self.logger.info("ObjectD running")