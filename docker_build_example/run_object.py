import argparse

"""
This script is meant to support a docker image to run a certain Object like sodocker run -it customer1/user1 /usr/bin/python3 /run_object.py -o ObjectA


"""


def run(package_and_module_path_to_import=None):
    """
    Take in a string such as 'my_package.my_module.ObjectA' and import
    ObjectA from my_package.my_module

    :param package_and_module_path_to_import: path to an package.module.object
    :return: None, run an instance of the object
    """
    obj_to_run = package_and_module_path_to_import.split('.')[-1]
    # ObjectA

    module_name = '.'.join(package_and_module_path_to_import.split('.')[:-1])
    # 'my_package.my_module'

    imported_module = __import__(module_name, globals(), locals(), ['object'])
    # a reference to module that has been imported
    # <module 'my_package.my_module' from  ... >

    object_class = getattr(imported_module, obj_to_run)
    # Reference of defintion of objectA, i.e., my_package.my_module.ObjectA

    obj_inst = object_class()
    # Instantiate that object. Constructor will run the object with obj_inst.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run an Arg')
    parser.add_argument('-p', '--object-to-run', )

    args = parser.parse_args()
    run(package_and_module_path_to_import=args.object_to_run)
