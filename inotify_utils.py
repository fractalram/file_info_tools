import json
import os
import time
import signal
import sys

import db
import checksum

harvest_killed = False

### FUCTIONS COMMON TO FILE HARVEST, INOTIFY, QUERYING ###

def get_settings():
    settings = None
    try:
        with open('inotify.conf', 'r') as f:
            settings = json.load(f)
        watch_dirs = []
        exclude_dirs = []
        with open('inotify_from_file.conf', 'r') as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith('#'):
                    continue
                if line.startswith('@'):
                    exclude_dirs.append(line.strip()[1:])
                else:
                    watch_dirs.append(line.strip())
        settings['watch_dirs'] = watch_dirs
        settings['exclude_dirs'] = exclude_dirs
    except Exception, e:
        return None, 'Error loading inotify settings : %s'%str(e)
    else:
        return settings, None

def get_db_location(settings = None):
    location = None
    try:
        if not settings:
            settings, err = get_settings()
            if err:
                raise Exception(err)
        if not settings:
            raise Exception('No inotify settings found')
        if 'db_location' not in settings:
            raise Exception('Database location not found in inotify settings')
        if not os.path.exists(settings['db_location']):
            raise Exception('Database not found in the location specified in inotify settings')

        location = settings['db_location']

    except Exception, e:
        return None, 'Error loading inotify DB location : %s'%str(e)
    else:
        return location, None


def get_file_info_row(db_path, path):
    row = False
    try:
        query = 'select * from file_info where path="%s"'%path
        row, err = db.get_single_row(db_path, query)
        if err:
            raise Exception(err)
    except Exception, e:
        return None, 'Error retrieving file info row : %s'%str(e)
    else:
        return row, None

### FILE INFO HARVEST FUNCTIONS - ALL FUNCTIONS BEGIN WITH fih_ ###

def fih_signal_handler(signum, stack):
    try:
        print 'Received a terminate signal so cleaning up and exiting.'
        global harvest_killed
        harvest_killed = True
    except Exception, e:
        print 'Error handling exception : %s'%str(e)
    else:
        return True, None

def fih_log_run_start(db_location, run_id, watch_dirs, exclude_dirs, generate_checksum, record_history, db_transaction_size, pid):
    new_run_id = 0
    try:
        initiate_time = int(time.time())
        watch_dirs_str = ','.join(watch_dirs)

        exclude_dirs_str = None
        if exclude_dirs:
            exclude_dirs_str = ','.join(exclude_dirs)
        if generate_checksum:
            generate_checksum_int = 1
        else:
            generate_checksum_int = 0
        if record_history:
            record_history_int = 1
        else:
            record_history_int = 0
        if run_id == 0:
            cmd = ['insert into file_info_harvest_runs(initiate_time, watch_dirs, exclude_dirs, generate_checksum, record_history, db_transaction_size, pid, status_id) values (?,?,?,?,?,?,?,?)', (initiate_time, watch_dirs_str, exclude_dirs_str, generate_checksum_int, record_history_int, db_transaction_size,pid, 1,)]
        else:
            cmd = ['update file_info_harvest_runs set initiate_time=?, pid=?, status_id=1, status_str=? where id = ?', (initiate_time, pid, None, run_id,)]
        row_id, err = db.execute_iud(db_location, [cmd], get_rowid=True)
        if err:
            raise Exception(err)
        if run_id == 0:
            new_run_id = row_id
        else:
            new_run_id = run_id
    except Exception, e:
        return None, 'Error logging harvest run start information : %s'%str(e)
    else:
        return new_run_id, None

def fih_log_run_progress(db_location, run_id, status_id, status_str, scanned_dir_count, scanned_file_count, successful_file_count, failed_file_count):
    try:
        if status_id == 3:
            #It is a pause so dont log the counts..
            cmd = ['update file_info_harvest_runs set status_id=?, status_str=? where id = ?', (status_id, status_str, run_id,)]
        else:
            cmd = ['update file_info_harvest_runs set status_id=?, status_str=?, scanned_dirs_count=?, scanned_files_count=?, successful_files_count=?, failed_files_count=? where id = ?', (status_id, status_str, scanned_dir_count, scanned_file_count, successful_file_count, failed_file_count, run_id,)]
        ret, err = db.execute_iud(db_location, [cmd], get_rowid=False)
        if err:
            raise Exception(err)
    except Exception, e:
        return False, 'Error logging harvest run completion information : %s'%str(e)
    else:
        return True, None

def fih_get_running_process_pid():
    pid = 0
    try:
        if os.path.exists('/var/run/integralstor/file_info_harvest'):
            with open('/var/run/integralstor/file_info_harvest', 'r') as f:
                pid_str = f.read()
            if pid_str:
                pid = int(pid_str)
        if pid > 0:
            try:
                os.kill(pid, 0)
            except Exception, e:
                #The process no longer exists so we can return a 0
                try:
                    os.remove('/var/run/integralstor/file_info_harvest')
                except Exception, e:
                    pass
                pid = 0

    except Exception, e:
        return None, 'Error checking for existing harvest process : %s'%str(e)
    else:
        return pid, None


def fih_execute(db_location, watch_dirs, exclude_dirs, run_id, db_transaction_size = 1000, generate_checksum = False, record_history = False):
    error_list = []
    successful_file_count = 0
    scanned_file_count = 0
    failed_file_count = 0
    scanned_dir_count = 0
    try:

        global harvest_killed
        harvest_killed = False
        print '-----------------------------SETTINGS---------------------------------'

        if generate_checksum:
            print 'Generating file checksums'
        else:
            print 'Not generating file checksums'
        if record_history:
            print 'Recording file event history'
        else:
            print 'Not recording file event history'
        dir_str = 'Collecting statistics for the following directories : %s..\n'%', '.join(watch_dirs)
        if exclude_dirs:
            dir_str += '..but excluding the following directories : %s'%', '.join(exclude_dirs)
        print dir_str
        print '----------------------------------------------------------------------'


        pid = os.getpid()
        new_run_id, err = fih_log_run_start(db_location, run_id, watch_dirs, exclude_dirs, generate_checksum, record_history, db_transaction_size, pid)
        if err:
            raise Exception(err)

        rerun = False
        if run_id > 0:
            rerun = True
        else:
            run_id = new_run_id

        counter = 0
        cmd_list = []
        transaction_file_list = []
        for watch_dir in watch_dirs:
            initiate = True
            if not os.path.exists(watch_dir):
                print 'Specified directory %s does not exist so continuing to the next one.'%watch_dir
                continue
            for root, dirs, files in os.walk(unicode(watch_dir)):
                if harvest_killed:
                    break
                if initiate:
                    print 'Collecting information about the directory structure for %s..'%watch_dir
                    initiate = False
                scanned_dir_count += 1
                #print 'Processing directory %s'%root
                if exclude_dirs and root in exclude_dirs:
                    print 'Skipping excluded directory : %s'%root
                    continue
                for file in files:
                    full_path = os.path.normpath('%s/%s'%(root, file))
                    #print full_path
                    try:
                        db_to_be_updated = False
                        scanned_file_count += 1
                        transaction_file_list.append(full_path)
                        if os.path.islink(full_path):
                            continue
                        extension = None
                        if full_path:
                            rt, extension = os.path.splitext(full_path)
                        mtime = os.path.getmtime(full_path)
                        #print 'mtime', mtime
                        size = os.path.getsize(full_path)
                        chksum = None
                        if rerun:
                            query = 'select * from file_info where path = "%s" and harvest_run_id = "%d"'%(full_path, run_id)
                            file_info_row, err = db.get_single_row(db_location, query)
                            if err:
                                raise Exception(err)
                        insert_update_file_info = True
                        if rerun and file_info_row:
                            #Rerun and it has been updated in the previous run of the same run id so skip
                            insert_update_file_info = False
                        if insert_update_file_info:
                            #This file has NOT been processed already in a previous run of the same run_id 
                            if generate_checksum:
                                chksum, err = checksum.generate_checksum(full_path, algorithm = 'sha256')
                                if err:
                                    error_list.append((transaction_file, err))
                                    print err
                                    continue
                                update_cmd = ['update file_info set size=?, last_modify_time=?, last_access_time=?, extension=?, checksum=?, harvest_run_id = ? where path = ?', (size, int(mtime), int(mtime), extension, chksum, run_id, full_path,)]
                            else:
                                update_cmd = ['update file_info set size=?, last_modify_time=?, last_access_time=?, extension=?, harvest_run_id=? where path = ?', (size, int(mtime), int(mtime), extension, run_id, full_path,)]
                            cmd = ['insert or ignore into file_info(path, extension, size, checksum, last_modify_time, last_access_time, harvest_run_id) values (?,?,?,?,?,?,?)', (full_path, extension, size, chksum, int(mtime), int(mtime),run_id,)]
                            cmd_list.append(cmd)
                            cmd_list.append(update_cmd)
                            db_to_be_updated = True
                        if record_history:
                            if rerun:
                                query = 'select * from file_events_history where path = "%s" and harvest_run_id = "%d"'%(full_path, run_id)
                                file_events_history_row, err = db.get_single_row(db_location, query)
                                if err:
                                    raise Exception(err)
                            insert_update_file_events_history = True
                            if rerun and file_events_history_row:
                                #Rerun and it has been updated in the previous run of the same run id so skip
                                insert_update_file_events_history = False
                            if insert_update_file_events_history:
                                cmd = ['insert or ignore into file_events_history(file_info_id, path, events, event_time, harvest_run_id) values ((select id from file_info where path=?),?,?,?,?)', (full_path, full_path, 'MODIFY', int(mtime), run_id,)]
                                cmd_list.append(cmd)
                                db_to_be_updated = True
                        if db_to_be_updated:
                            counter += 1
                        if cmd_list and (counter != 0) and (counter % db_transaction_size == 0):
                            print 'Scanned %d files'%scanned_file_count
                            #print cmd_list
                            ret, err = db.execute_iud(db_location, cmd_list, get_rowid=False)
                            #print ret, err
                            if err:
                                failed_file_count += counter
                                for transaction_file in transaction_file_list:
                                    error_list.append((transaction_file, 'Error inserting/updating into the database : %s'%err))
                            else:
                                successful_file_count += counter
                                ret, err = fih_log_run_progress(db_location, run_id, 1, 'Scanned %d directories and %d files. Processed %d files successfully with %d errors'%(scanned_dir_count, scanned_file_count, successful_file_count, failed_file_count), scanned_dir_count, scanned_file_count, successful_file_count, failed_file_count)
                            cmd_list = []
                            transaction_file_list = []
                            counter = 0
                    except Exception, e:
                        #print e
                        error_list.append((full_path, str(e)))
            if harvest_killed:
                break
        if not harvest_killed:
            if cmd_list:
                print 'Processing the last batch of %d files.'%counter
                #Still have unprocessed files so insert them!
                ret, err = db.execute_iud(db_location, cmd_list, get_rowid=False)
                #print ret, err
                if err:
                    failed_file_count += counter
                    for transaction_file in transaction_file_list:
                        error_list.append((transaction_file, 'Error inserting into the database : %s'%err))
                else:
                    successful_file_count += counter
            ret, err = fih_log_run_progress(db_location, run_id, 2, 'Scanned %d directories and %d files. Processed %d files successfully with %d errors'%(scanned_dir_count, scanned_file_count, successful_file_count, failed_file_count), scanned_dir_count, scanned_file_count, successful_file_count, failed_file_count)
        else:
            time_str = time.strftime('%a, %d %b %Y %H:%M:%S')
            ret, err = fih_log_run_progress(db_location, run_id, 3, 'Paused at %s'%time_str, 0,0,0,0)
    except Exception, e:
        #print e
        return False, 'Error collecting statistics : %s'%str(e), error_list, -1, -1, -1, -1
    else:
        return True, None, error_list, scanned_dir_count, scanned_file_count, successful_file_count, failed_file_count





### INOTIFY RELATED FUNCTIONS ###

def get_pending_events(db_path):
    event_rows = False
    try:
        query = 'select * from inotify_events'
        event_rows, err = db.get_multiple_rows(db_path, query)
        if err:
            raise Exception(err)
    except Exception, e:
        return None, 'Error retrieving pending events from the database: %s'%str(e)
    else:
        return event_rows, None

def record_file_events_history(db_path, path, event_row, file_info_id = None, get_file_info_id = False, moved_from_path = None):
    ret = False
    try:
        if get_file_info_id:
            file_info_row, err = get_file_info_row(db_path, path)
            if file_info_row:
                file_info_id = file_info_row['id']
        cmd = ['insert or ignore into file_events_history(file_info_id, path, events, event_time, moved_from_path) values (?,?,?,?,?)', (file_info_id, path, event_row['events'], event_row['event_time'],moved_from_path,)]
        ret, err = db.execute_iud(db_path, [cmd], get_rowid=False)
        if err:
            raise Exception(err)
    except Exception, e:
        st = 'Error recording file events history '
        if path:
            st += 'for %s '%path
        st += ': %s'%str(e)
        return False, st
    else:
        return True, None

def process_create_modify_access_attrib_event(event_row, file_info_row, file_exists, path, extension, size, chksum, events):
    cmds = None
    try:
        cmd = None
        if file_exists:
            if file_info_row:
                if 'CREATE' in events or 'MODIFY' in events:
                    cmd = ['update file_info set size=?, last_modify_time=?, last_access_time=?, extension=?, checksum=?, harvest_run_id=0 where id = ?', (
                            size, event_row['event_time'], event_row['event_time'], extension, chksum, file_info_row['id'],)]
                elif 'ACCESS' in events:
                    cmd = ['update file_info set size=?, extension=?, last_access_time, last_read_time=?, checksum=?, harvest_run_id=0 where id = ?', (
                        size, extension, event_row['event_time'], event_row['event_time'], chksum, file_info_row['id'],)]
                elif 'ATTRIB' in events:
                    cmd = ['update file_info set extension=?, last_attrib_modify_time=?, harvest_run_id=0 where id = ?', (
                                extension, event_row['event_time'], file_info_row['id'],)]
            else:
                if 'CREATE' in events or 'MODIFY' in events:
                    cmd = ['insert into file_info(path, extension, size, checksum, last_modify_time, last_access_time, harvest_run_id) values (?,?,?,?,?,?,0)', (path, extension, size, chksum, event_row['event_time'], event_row['event_time'],)]
                elif 'ACCESS' in events:
                    cmd = ['insert into file_info(path, extension, size, checksum, last_read_time, last_access_time, harvest_run_id) values (?,?,?,?,?,?,0)', (path, extension, size, chksum, event_row['event_time'], event_row['event_time'], event_row['event_time'],)]
                elif 'ATTRIB' in events:
                    cmd = ['insert into file_info(path, extension, size, last_attrib_modify_time, checksum, last_modify_time, harvest_run_id) values (?,?,?,?,?,?,0)', (path, extension, size, event_row['event_time'], chksum, event_row['event_time'], )]
        else:
            #File has been deleted in the meanwhile so remove it from file_info
            if file_info_row:
                cmd = ['delete from file_info id = "%d"'%file_info_row['id']]
            else:
                #Nothing to do since it has not been recorded in the file_info anyway..
                pass
        if cmd:
            cmds = [cmd]
    except Exception, e:
        return None, 'Error processing access event : %s'%str(e)
    else:
        return cmds, None

def process_delete_event(file_info_row):
    cmds = None
    try:
        if file_info_row:
            cmds = [['delete from file_info where id = "%d"'%file_info_row['id']]]
    except Exception, e:
        return None, 'Error processing delete event : %s'%str(e)
    else:
        return cmds, None

def process_moved_to_event(event_row, file_info_row, file_exists, path, extension, size, chksum, events, moved_from_path, moved_from_file_info_id, moved_from_inotify_events_id):
    cmds = []
    try:
        cmd = None
        if not moved_from_path:
            raise Exception('Cold not retrieve moved from path!')
        if file_info_row:
            #The new path exists but we need to update it with the new info
            cmd = ['update file_info set extension=?, last_modify_time=%d, last_access_time, size=%d, checksum="%s", harvest_run_id=0 where id = %d'%(extension, event_row['event_time'], event_row['event_time'], size, chksum, file_info_row['id'])]
        else:
            #The new path does not exist so we need to create it with the new info
            cmd = ['insert into file_info(path, extension, size, checksum, last_modify_time, last_access_time,harvest_run_id) values (?,?,?,?,?,?,0)', (path, extension, size, chksum, event_row['event_time'], event_row['event_time'],)]
        if cmd:
            cmds.append(cmd)
        if moved_from_file_info_id:
            #Delete the one corresponding to the moved from..
            cmd = ['delete from file_info where id="%s"'%moved_from_file_info_id]
            cmds.append(cmd)
        #Processed a move to so also go back and delete the moved from event
        if not moved_from_inotify_events_id:
            print 'Something fishy happened!'
        else:
            cmd = ['delete from inotify_events where id="%d"'%moved_from_inotify_events_id]
            cmds.append(cmd)
    except Exception, e:
        return None, 'Error processing moved_to event : %s'%str(e)
    else:
        return cmds, None


def process_events(settings = None):
    try:
        if not settings:
            settings, err = get_settings()
            if err:
                raise Exception(err)

        if not settings:
            raise Exception('No inotify settings found')

        db_path, err = get_db_location(settings)
        if err:
            raise Exception(err)

        event_rows, err = get_pending_events(db_path)
        if err:
            raise Exception(err)

        generate_checksum = False 
        if 'generate_checksum' in settings:
            generate_checksum = settings['generate_checksum']

        record_history = False 
        if 'record_file_events_history' in settings:
            record_history = settings['record_file_events_history']

        if event_rows:
            cmd_list = []
            moved_from_path = None
            moved_from_file_info_id = None
            clear_moved_from_info = False
            record_moved_from_info = False
            for event_row in event_rows:
                print 'Processing row : ',event_row
                events = event_row['events'].upper()
                path = event_row['path']
                extension = None
                if path:
                    root, extension = os.path.splitext(path)
                file_exists = False
                delete_inotify_event = False
                chksum = None
                file_info_row, err = get_file_info_row(db_path, path)
                if err:
                    raise Exception(err)
                cmds = []
                size = 0
                if os.path.exists(path):
                    #File still exists so we shd get the new size
                    size = os.path.getsize(path)
                    file_exists = True
                    if generate_checksum and 'MODIFY' in events or 'CREATE' in events:
                        chksum, err  = checksum.generate_checksum(path, algorithm = 'sha256')
                        if err:
                            raise Exception(err)
                if 'MODIFY' in events or 'CREATE' in events or 'ACCESS' in events or 'ATTRIB' in events:
                    cmds, err = process_create_modify_access_attrib_event(event_row, file_info_row, file_exists, path, extension, size, chksum, events)
                    if err:
                        raise Exception(err)
                    delete_inotify_event = True
                elif 'DELETE' in events:
                    cmds, err = process_delete_event(file_info_row)
                    if err:
                        raise Exception(err)
                    delete_inotify_event = True
                elif 'MOVED_FROM' in events:
                    #Save the moved from info to process the next moved_to event which hopefully corresponds to this one!
                    moved_from_path = path
                    clear_moved_from_info = False
                    moved_from_inotify_events_id = event_row['id']
                    if file_info_row:
                        moved_from_file_info_id = file_info_row['id']
                    delete_inotify_event = False
                elif 'MOVED_TO' in events:
                    delete_inotify_event = True
                    #Processed the move so remove the old one..
                    cmds, err = process_moved_to_event(event_row, file_info_row, file_exists, path, extension, size, chksum, events, moved_from_path, moved_from_file_info_id, moved_from_inotify_events_id)
                    if err:
                        raise Exception(err)
                    clear_moved_from_info = True
                    record_moved_from_info = True

                if cmds:
                    cmd_list.extend(cmds)
                if delete_inotify_event:
                    #Delete the event that was just processed
                    #print 'Deleting inotify event : %s'%cmd
                    cmd = ['delete from inotify_events where id="%d"'%event_row['id']]
                    cmd_list.append(cmd)
                if cmd_list:
                    ret, err = db.execute_iud(db_path, cmd_list, get_rowid=False)
                    if err:
                        raise Exception(err)
                    cmd_list = []
                if record_history:
                    mfp = None
                    if record_moved_from_info:
                        mfp = moved_from_path
                    ret, err = record_file_events_history(db_path, path, event_row, get_file_info_id = True, moved_from_path = mfp)
                    if err:
                        raise Exception(err)
                if clear_moved_from_info:
                    moved_from_path = None
                    moved_from_file_info_id = None
                    moved_from_inotify_events_id = None
                    record_moved_from_info = False
    except Exception, e:
        return False, 'Error processing events : %s'%str(e)
    else:
        return True, None




### FILE INFO QUERYING FUNCTIONS ###

def simple_file_info_query(query_type, db_location = None, limit = 10):
    ret_list = []
    try:
        if query_type not in ['NEWEST_READ_TIME', 'NEWEST_MODIFY_TIME', 'NEWEST_ACCESS_TIME', 'OLDEST_READ_TIME', 'OLDEST_MODIFY_TIME', 'OLDEST_ACCESS_TIME', 'MOST_RECENTLY_ATTRIB', 'LARGEST']:
            raise Exception('Unknown query type')
        if not db_location:
            db_location, err = get_db_location()
            if err:
                raise Exception(err)
        query_mapping = { 'NEWEST_READ_TIME' : 'select * from file_info where id != 0 and last_read_time is not null order by last_read_time desc limit %d'%limit,
                            'OLDEST_READ_TIME' : 'select * from file_info where id != 0 and last_read_time is not null order by last_read_time limit %d'%limit,
                            'NEWEST_MODIFY_TIME' : 'select * from file_info where id != 0 and last_modify_time is not null order by last_modify_time desc limit %d'%limit,
                            'OLDEST_MODIFY_TIME' : 'select * from file_info where id != 0 and last_modify_time is not null order by last_modify_time limit %d'%limit,
                            'NEWEST_ACCESS_TIME' : 'select * from file_info where id != 0 and last_access_time is not null order by last_access_time desc limit %d'%limit,
                            'OLDEST_ACCESS_TIME' : 'select * from file_info where id != 0 and last_access_time is not null order by last_access_time limit %d'%limit,
                            'MOST_RECENTLY_ATTRIB' : 'select * from file_info where id != 0 and last_attrib_modify_time is not null order by last_attrib_modify_time limit %d'%limit,
                            'LARGEST' : 'select * from file_info where id != 0 and size is not null order by size desc limit %d'%limit }
        query = query_mapping[query_type]
        ret_list, err = db.get_multiple_rows(db_location, query)
        if err:
            raise Exception(err)
    except Exception, e:
        return None, 'Error querying the database : %s'%str(e)
    else:
        return ret_list, None

def get_duplicate_files(db_location=None):
    ret_list = []
    try:
        if not db_location:
            db_location, err = get_db_location()
            if err:
                raise Exception(err)
        query = 'select checksum, size, count(checksum) as dup_count from file_info group by checksum, size order by size desc, dup_count desc'
        results, err = db.get_multiple_rows(db_location, query)
        if err:
            raise Exception(err)
        #print results[:5]
        for result in results:
            dup_dict = {}
            if result['checksum'] == None:
                continue
            query = 'select * from file_info where checksum = "%s"'%result['checksum']
            checksum_results, err = db.get_multiple_rows(db_location, query)
            if err:
                raise Exception(err)
            dup_list = []
            for checksum_result in checksum_results:
                dup_list.append({'file_name': checksum_result['path'], 'size':checksum_result['size']})
            dup_dict['count'] = result['dup_count']
            dup_dict['size'] = result['size']
            dup_dict['dup_list'] = dup_list
            ret_list.append(dup_dict)
        
    except Exception, e:
        return None, 'Error retrieving duplicate files : %s'%str(e)
    else:
        return ret_list, None

def get_extension_count(db_location=None):
    ret_list = []
    try:
        if not db_location:
            db_location, err = get_db_location()
            if err:
                raise Exception(err)
        query = 'select extension, count(*) as count from file_info group by extension having (count(*) > 0 and id != 0) order by count desc'
        ret_list, err = db.get_multiple_rows(db_location, query)
        if err:
            raise Exception(err)
    except Exception, e:
        return None, 'Error retrieving file extension counts : %s'%str(e)
    else:
        return ret_list, None

def get_file_count(db_location=None):
    ret_dict = None
    try:
        if not db_location:
            db_location, err = get_db_location()
            if err:
                raise Exception(err)
        query = 'select count(*) as count from file_info where id != 0'
        ret_dict, err = db.get_single_row(db_location, query)
        if err:
            raise Exception(err)
    except Exception, e:
        return None, 'Error retrieving file count : %s'%str(e)
    else:
        return ret_dict, None

def get_files_by_extension(extension = '', db_location = None):
    ret_list = []
    try:
        if not db_location:
            db_location, err = get_db_location()
            if err:
                raise Exception(err)
        query = 'select * from file_info where id != 0 and extension = "%s"'%extension
        ret_list, err = db.get_multiple_rows(db_location, query)
        if err:
            raise Exception(err)
    except Exception, e:
        return None, 'Error retrieving files by  extension : %s'%str(e)
    else:
        return ret_list, None

def main():
    #print get_settings()
    #print get_results('select * from file_info where id != 0 and last_read_time is not null order by last_read_time limit 10')
    #print simple_file_info_query('LARGEST')
    #print simple_file_info_query('NEWEST_ACCESS_TIME')
    #print get_duplicate_files()
    #print get_extension_count()
    print fih_get_running_process_pid()

if __name__ == '__main__':
    main()
