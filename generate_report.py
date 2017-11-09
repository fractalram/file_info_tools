import inotify_utils
import datetime

def generate_report(count=10):
    try:
        lines = []
        db_location, err = inotify_utils.get_db_location()
        if err:
            raise Exception(err)
        rd, err = inotify_utils.get_file_count(db_location)
        if err:
            raise Exception(err)
        lines.append('\nFILE COUNT INFORMATION\n')
        lines.append('======================\n')
        lines.append('Total number of files in the database : %d\n\n'%rd['count'])
        res_list, err = inotify_utils.get_extension_count()
        if err:
            raise Exception(err)
        lines.append('\nFILE EXTENSION INFORMATION\n')
        lines.append('==========================\n')
        for result in res_list:
            if result['extension']:
                lines.append('Extension : "%s" File count : %d\n'%(result['extension'], result['count']))
            else:
                lines.append('Extension : None File count : %d\n'%(result['count']))
        lines.append('\n')

        res_list, err = inotify_utils.simple_file_info_query('LARGEST', db_location)
        if err:
            raise Exception(err)
        lines.append('\nLARGEST FILES \n')
        lines.append('============= \n')
        for result in res_list:
            size = ((float(result['size'])/1024)/1024)/1024
            lines.append('File name : %s. Size : %.2f GB\n'%(result['path'], size))
        lines.append('\n')

        res_list, err = inotify_utils.simple_file_info_query('OLDEST_MODIFY_TIME', db_location)
        if err:
            raise Exception(err)
        lines.append('\nFILES WITH OLDEST MODIFY TIMES \n')
        lines.append('============================== \n')
        for result in res_list:
            time = datetime.datetime.fromtimestamp(
                    int(result['last_modify_time'])
                        ).strftime('%Y-%m-%d %H:%M:%S')
            lines.append('File name : %s. Modified on : %s\n'%(result['path'], time))
        lines.append('\n')

        res_list, err = inotify_utils.simple_file_info_query('NEWEST_MODIFY_TIME', db_location)
        if err:
            raise Exception(err)
        lines.append('\nFILES WITH NEWEST MODIFY TIMES \n')
        lines.append('============================== \n')
        for result in res_list:
            time = datetime.datetime.fromtimestamp(
                    int(result['last_modify_time'])
                        ).strftime('%Y-%m-%d %H:%M:%S')
            lines.append('File name : %s. Modified on : %s\n'%(result['path'], time))
        lines.append('\n')

        res_list, err = inotify_utils.get_duplicate_files()
        if err:
            raise Exception(err)

        lines.append('\nFILES WITH SAME CHECKSUMs (MOST PROBABLY DUPLICATES)\n')
        lines.append('====================================================\n')
        if res_list:
            count = 1
            for result in res_list:
                lines.append('\nSet %d. Number of files with duplicate checksums : %d\n'%(count, result['count']))
                lines.append('-----------------------------------------------------\n')
                for file in result['dup_list']:
                    size = float(file['size'])/1024
                    lines.append('File size : %.2fKB, File name : %s\n'%(size, file['file_name']))
                count += 1
        else:
            lines.append('No potential duplicate files found\n')

        with open('inotify_report.txt', 'w') as f:
            for line in lines:
                f.write(line.encode('utf-8'))
        #print ''.join(lines)

    except Exception, e:
        return False, 'Error generating report : %s'%str(e)
    else:
        return True, None

if __name__ == '__main__':
    ret, err = generate_report()
    if err:
        print err
