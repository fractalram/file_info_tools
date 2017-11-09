import sys

import inotify_utils

if __name__ == '__main__':
    ret, err = inotify_utils.process_events()
    if err:
        print err
        sys.exit(-1)
    else:
        sys.exit(0)
