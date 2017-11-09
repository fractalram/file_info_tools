#!/bin/sh

INOTIFY_FROM_FILE="inotify_from_file.conf"
INOTIFY_RECORD_SCRIPT="record_inotify_events.py"

if [ $# -ne 2 ]
then
    echo "Usage initiate_inotify.sh <inotify_conf_dir> <python_scripts_path>"
    exit 0
fi

if [ ! -d $2 ]
then
    echo "Invalid python scripts path provided. Exiting.."
    exit 1
fi

if [ ! -f "$2/$INOTIFY_RECORD_SCRIPT" ]
then
    echo "The python scripts path provided does not contain the recording script. Exiting.."
    exit 1
fi

if [ ! -d $1 ]
then
    echo "The inotify configuration path does not exist. Exiting.."
    exit 1
fi

if [ ! -f "$1/$INOTIFY_FROM_FILE" ]
then
    echo "The python configuration directory does not contain a file by the name $INOTIFY_FROM_FILE which has the list of directories to monitor. Exiting.."
    exit 1
fi

#inotifywait -mr @$exclude_dir --timefmt '%s' --format '%T %:e %w::::%f' "$watch_dir" | while read time event event_path ; do
inotifywait -mr --fromfile "$1"/$INOTIFY_FROM_FILE --timefmt '%s' --format '%T %:e %w::::%f' | while read time event event_path ; do
   #echo "At ${time}, path ${event_path} event ${event} was generated"
   python $2/$INOTIFY_RECORD_SCRIPT ${time} "${event_path}" ${event}
done
