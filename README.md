# libdag

Library implementation of core Whiskey Cube logic -- data pipelines without coding.

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
can examine this with deepdish like so

    ipython3
    import deepdish as dd
    dd.io.load('<h5file path>')
    {'objectD': array([ 5.,  5.,  5.,  5.,  5.,  5.,  5.])}

    
### Main Feature

You want to create a data computation pipeline but not have to worry about infrastructure. The solution
is to automate as much of the plumbing backend work as possible. This project was inspired by the
likes of Facebook's fantastic [FBLearner](https://code.facebook.com/posts/1072626246134461/introducing-fblearner-flow-facebook-s-ai-backbone/)
 and my favorite, Spotify's [Luigi](https://github.com/spotify/luigi/).
 
The niche Whiskey Cube will solve is to make experimenting computation as easy as possible. No longer
 do you need to worry about how the pipeline you have will work or not. So long as your code inside
 the node runs and is checked into a git repo somewhere, Whiskey Cube will work.
 
 At worse, you will have to duplicate or fork a graph, and maybe some of the code inside of the nodes, 
 but they will always live in their own universe.
 