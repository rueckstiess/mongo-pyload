import pymongo
import argparse
import multiprocessing
import time


def worker(number):
    """ bulk insert via generator into the collection. """
    coll[number].insert( ({} for i in xrange(batchsize)), manipulate=False )


if __name__ == '__main__':

    # multiprocessing
    numcores = multiprocessing.cpu_count()

    parser = argparse.ArgumentParser(description="bulk insert empty documents into MongoDB")
    parser.add_argument('--number', metavar='N', default=10000000, type=int, help='number of documents to insert [default: 10M]')
    parser.add_argument('--batchsize', metavar='B', default=1000, type=int, help='batch size for single bulk insert [default: 1000]')
    parser.add_argument('--database',  metavar='DB', default='pyload', help='database to insert into [default: pyload]')
    parser.add_argument('--collection', metavar='COLL', default='coll', help='collection (prefix) to insert into [default: coll]')
    parser.add_argument('--host', default='localhost', help='host where MongoDB is located [default: localhost]')
    parser.add_argument('--port', default=27017, type=int, help='port to connect to MongoDB [default: 27017]')
    parser.add_argument('--write-concern', metavar='W', default=0, type=int, help='write concern [default: 0]')
    parser.add_argument('--round-robin', metavar='R', default=10, type=int, help='use multiple collections round-robin [default: 10]')
    parser.add_argument('--threads', metavar='T', default=numcores, type=int, help='number of simultaneous threads [default: #cores = %i]' % numcores)

    print "using pymongo version %s" % pymongo.version

    # variables
    number = 10000000
    batchsize = 1000
    port = 27017
    database = "test"
    coll_prefix = "coll"
    num_collections = 10

    args = parser.parse_args()
        
    # mongodb
    mc = pymongo.MongoClient(host=args.host, port=args.port)
    mc.write_concern = {'w': args.write_concern};
    coll = [ mc[args.database][args.collection+str(i)] for i in range(args.round_robin) ]
    for c in coll:
        c.drop()

    # run worker pool 
    t = time.time()
    pool = multiprocessing.Pool(processes=args.threads)
    pool.map(worker, (n % args.round_robin for n in xrange(args.number / args.batchsize)) )
    
    pool.close()
    pool.join()

    d = time.time() - t
    print "inserted %i docs into %i collections with %i threads in %f seconds = %f docs/sec" % (args.number, args.round_robin, args.threads, d, args.number/d)

    time.sleep(1)
    total = sum( c.count() for c in coll )
    print "counts on MongoDB report %i documents total." % total

