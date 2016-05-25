#!/bin/sh

cd $(dirname $0)
case "$1" in
    server)
        _SETTINGS_FILE=${SYNC_SETTINGS_FILE:-"/app/example.ini"}

        if [ ! -e $_SETTINGS_FILE ]; then 
            echo "Could not find ini file: $_SETTINGS_FILE"
            exit 1
        fi

        echo "Starting gunicorn with config: $_SETTINGS_FILE"

        exec gunicorn \
            --paste "$_SETTINGS_FILE" \
            --bind ${HOST-127.0.0.1}:${PORT-8000}\
            --worker-class mozsvc.gunicorn_worker.MozSvcGeventWorker \
            --timeout ${SYNC_TIMEOUT-600} \
            --workers ${WEB_CONCURRENCY-1}\
            --graceful-timeout ${SYNC_GRACEFUL_TIMEOUT-660}\
            --max-requests ${SYNC_MAX_REQUESTS-5000}\
            --log-config "$_SETTINGS_FILE"
        ;;

    test_all)
        $0 test_flake8
        $0 test_nose
        $0 test_functional
        ;;

    test_flake8)
        echo "test - flake8"
        flake8 syncstorage
        ;;

    test_nose)
        echo "test - nose"
        nosetests --verbose --nocapture syncstorage/tests
        ;;

    test_functional)
        echo "test - functional"
        # run functional tests
	    export MOZSVC_SQLURI=sqlite:///:memory: 
        gunicorn --paste ./syncstorage/tests/tests.ini \
            --workers 1 \
            --worker-class mozsvc.gunicorn_worker.MozSvcGeventWorker & 

        SERVER_PID=$! 
        sleep 2

        $0 test_endpoint http://localhost:5000

        kill $SERVER_PID
        ;;

    test_endpoint)
        python syncstorage/tests/functional/test_storage.py $2
        ;;

    *)
        echo "Unknown CMD, $1"
        exit 1
        ;;
esac
