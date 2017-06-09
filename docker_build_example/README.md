## Building a Docker Image to run With Diag

    
### Directory Structure

    .
    ├── Dockerfile
    ├── README.md
    ├── my_package
    │   ├── __init__.py
    │   ├── my_module.py
    │   ├── setup.py
    └── run_object.py
    
### Docker build command

    docker build --build-arg USER_PACKAGE=my_package -t mjk_api_key/my_package  .

### Docker run command

    docker run mjk_api_key/my_package /usr/bin/python3 /run_object.py -p my_package.my_module.ObjectA -u mjk_api_key -r test_run_key
    
