#!/bin/bash
BUILD_DIR=build

case $1 in
    build)
        cd $2
        mkdir $BUILD_DIR
        cd $BUILD_DIR
        cmake ..
        make -j8
        ;;
    deploy)
        APP=$3
        HOST=$4
        cp $2/$BUILD_DIR/src/$APP/$APP $APP
        cp /tmp/$APP.conf $APP.conf
        tar -cjvf $APP.tar.bz2 $APP $APP.conf
        cocaine-tool app upload --host $HOST --port 10056 -n $APP --manifest $APP.conf --package $APP.tar.bz2
        cocaine-tool profile upload --host $HOST --port 10056 -n $APP --profile /tmp/test_app.profile
        ;;
    start|stop|ping)
        APP=$2
        HOST="$3:1025:2"
        GROUP=$4
        DIOC="dnet_ioclient -r $HOST -g $GROUP"
        case $1 in
            start)
                $DIOC -c "$APP@start-task"
                ;;
            stop)
                $DIOC -c "$APP@stop-task"
                ;;
            ping)
                $DIOC -c "$APP@ping"
                echo
                ;;
        esac
        ;;
esac
