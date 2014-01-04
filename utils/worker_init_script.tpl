
case "$1" in
    start)
        if pgrep -x pworker
        then
                echo "worker is already running"
        else
                echo "Starting Worker server..."
                $EXEC --d --c $CONF --r $RESOURCES --u $WUID
        fi
        ;;
    stop)
        if pgrep -x pworker
        then
                echo "Stopping ..."
                $EXEC --s
                while [ `pgrep -x pworker` ]
                do
                    echo "Waiting for Worker to shutdown ..."
                    sleep 1
                done
                echo "Worker stopped"
        else
                echo "worker is not running"
        fi
        ;;
    *)
        echo "Please use start or stop as first argument"
        ;;
esac
