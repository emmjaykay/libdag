# libdag

Library to run Whiskey Cube -- data pipelines without coding.

Computation for Deep Learning models is organized as nodes in a graph.
Data flows between these nodes. Each node is contained in a Docker
container if you wish, either locally or remotely.

### Running an example

    mkdir libdag
    virtualenv -p python3 libdag
    cd libdag
    git <clone repo>
    cd docker_build_example
    mkdir data
    ln -s ./data /data    
    docker build --build-arg USER_PACKAGE=my_package -t mjk_api_key/my_package  .
    python3 ../dag.py -r user_api_key
    
The last file should output an h5 file somewhere in your `/data/<api_key>/<run_uuid_number>/`. You
can examine this with 

    
### Main Feature

You want to create a data computation pipeline but not have to worry about infrastructure. The solution
is to automate as much of the plumbing backend work as possible. 