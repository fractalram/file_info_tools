import sys
import os
import time
import signal

import inotify_utils

if __name__ == '__main__':
    try:

        signal.signal(signal.SIGTERM, inotify_utils.fih_signal_handler)

        settings, err = inotify_utils.get_settings()
        if err:
            raise Exception(err)


        db_location, err = inotify_utils.get_db_location(settings)
        if err:
            raise Exception(err)


        db_transaction_size = 1000
        if 'db_transaction_size' in settings:
            db_transaction_size = int(settings['db_transaction_size'])

        generate_checksum = False 
        if 'generate_checksum' in settings:
            generate_checksum = settings['generate_checksum']

        record_history = False 
        if 'record_file_events_history' in settings:
            record_history = settings['record_file_events_history']

        if 'watch_dirs' not in settings:
            raise Exception('No scan directories provided.')
        watch_dirs = settings['watch_dirs']

        exclude_dirs = []
        if 'exclude_dirs' in settings:
            exclude_dirs = settings['exclude_dirs']
        if 'exclude_dirs' in settings:
            exclude_dirs = settings['exclude_dirs']

        running_pid, err = inotify_utils.fih_get_running_process_pid()
        if err:
            raise Exception(err)
        if running_pid > 0:
            raise Exception('A file statistics harvest process with process id %d is currently running. Only one harvest process can run at one time. Exiting now.'%running_pid)

        run_id = 0
        if len(sys.argv) == 2:
            run_id = int(sys.argv[1])

        pid = os.getpid()
        with open('/var/run/integralstor/file_info_harvest', 'w') as f:
            f.write('%d'%pid)

        ret, err, error_list, scanned_dir_count, scanned_file_count, successful_file_count, failed_file_count = inotify_utils.fih_execute(db_location, watch_dirs, exclude_dirs, run_id = run_id, db_transaction_size = db_transaction_size, generate_checksum = generate_checksum, record_history = record_history)
        #print ret, err, error_list
        if error_list:
            print 'The following errors occurred during the collection process : '
            for (path, error) in error_list:
                print '%s : %s'%(path, error)
        if err:
            raise Exception('The harvest process failed to complete successfully : %s'%err)
        else:
            print 'The harvest process completed successfully.'
            print 'Scanned a total of %d directories and %d files. Successfully processed %d files with %d errors.'%(scanned_dir_count, scanned_file_count, successful_file_count, failed_file_count)
    except Exception, e:
        print str(e)
        sys.exit(-1)
    else:
        sys.exit(0)
    finally:
        os.remove('/var/run/integralstor/file_info_harvest')
