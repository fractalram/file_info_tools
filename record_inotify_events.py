import sys
import os

import db

def record_event(event_time, event_dir, events, is_file, event_file = None):
    try:
        if event_file:
            event_path = os.path.normpath(unicode('%s/%s'%(event_dir, event_file)))
        else:
            event_path = os.path.normpath(unicode(event_dir))
        if 'ISDIR' not in events and 'CLOSE' not in events and 'OPEN' not in events:
            cmd = ['insert into inotify_events(path, events, is_file, event_time) values (?,?,?,?)', (event_path, events, is_file, event_time,)]
            ret, err = db.execute_iud('inotify.db', [cmd], get_rowid=False)
            if err:
                raise Exception(err)
    except Exception, e:
        return False, 'Error recording event : %s'%str(e)
    else:
        return True, None

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print 'Usage: python inotify.py <time> <path> <events>'
        sys.exit(-1)
    events = sys.argv[3].upper()
    #print 'Time: %d'%int(sys.argv[1])
    #print 'Events : %s'%sys.argv[3]
    components = sys.argv[2].split('::::')
    #print 'components - ', components
    if 'ISDIR' in events or 'CLOSE' in events or 'OPEN' in events:
        print 'Unsupported events :  "%s" so skipping'%events
        sys.exit(0)
    if len(components) > 1 and components[1]:
        ret, err = record_event(int(sys.argv[1]), components[0], sys.argv[3], 1, components[1])
    else:
        ret, err = record_event(int(sys.argv[1]), components[0], sys.argv[3], 0)
    if err:
        print err
        sys.exit(-1)
    else:
        sys.exit(0)
