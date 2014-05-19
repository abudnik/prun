
case "$1" in
    start)
        if pgrep -x pmasterdb
        then
                echo "master db is already running"
        else
                echo "Starting Master db server..."
                $EXEC --d --c $CONF
        fi
        ;;
    stop)
        if pgrep -x pmasterdb
        then
                echo "Stopping ..."
                $EXEC --s
                while [ `pgrep -x pmasterdb` ]
                do
                    echo "Waiting for Master db to shutdown ..."
                    sleep 1
                done
                echo "Master db stopped"
        else
                echo "master db is not running"
        fi
        ;;
    *)
        echo "Please use start or stop as first argument"
        ;;
esac
