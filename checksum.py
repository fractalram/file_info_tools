
import hashlib
#Attribution : Omnifarious in https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file

def hash_bytestr_iter(bytesiter, hasher, ashexstr=False):
    for block in bytesiter:
        hasher.update(block)
    return (hasher.hexdigest() if ashexstr else hasher.digest())

def file_as_blockiter(afile, blocksize=65536):
#def file_as_blockiter(afile, blocksize=655360):
    with afile:
        block = afile.read(blocksize)
        while len(block) > 0:
            yield block
            block = afile.read(blocksize)

def generate_checksum(fname, algorithm = 'sha256'):
    checksum = None
    try:
        if algorithm and algorithm == 'sha256':
            fn = hashlib.sha256()
        else:
            raise Exception('Unsupported checksum algorith')
        checksum = hash_bytestr_iter(file_as_blockiter(open(fname, 'rb')), hashlib.sha256(), ashexstr = True)
    except Exception, e:
        return None, 'Error generating checksum : %s'%str(e)
    else:
        return checksum, None
