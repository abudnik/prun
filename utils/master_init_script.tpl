
case "$1" in
    start)
        if pidof pmaster
        then
                echo "master is already running"
        else
                echo "Starting Master server..."
                $EXEC --d --c $CONF
        fi
        ;;
    stop)
        if pidof pmaster
        then
                echo "Stopping ..."
                $EXEC --s
                while [ `pidof pmaster` ]
                do
                    echo "Waiting for Master to shutdown ..."
                    sleep 1
                done
                echo "Master stopped"
        else
                echo "master is not running"
        fi
        ;;
    *)
        echo "Please use start or stop as first argument"
        ;;
esac
