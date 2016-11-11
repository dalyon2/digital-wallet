#Written in 2016 by David Lyon
#for the Insight Data Engineering coding challenge
import codecs,sys,datetime

friends1={}
friends2={}
headers=[]

#Make a dictionary of all users containing the set of everyone either sending to or receiving from that user

def trainnetwork1(payfile):
    with codecs.open(payfile,'r','utf-8') as f:
        headers=f.readline().split(',')
        for line in f:
            payment=line.split(',')
            if len(payment) < 5:                
            #make sure there are at least 5 fields in the payment, can be more than 5 because of commas in the comment
                continue
            else:
                try:
                    #make sure the user ids are integers
                    id1=int(payment[1])
                    id2=int(payment[2])
                    if id1 not in friends1:
                        friends1[id1]=set([id2])
                    else:
                        friends1[id1].add(id2)
                    if id2 not in friends1:
                        friends1[id2] = set([id1])
                    else:
                        friends1[id2].add(id1)
                except:
     #               print('Malformed batch payment: ' + line)
                     continue
    f.close()
    print('Network1 trained!')

#Make a dictionary of all friends of friends of a given user. This can be used twice consecutively for the degree 4 network as well
    
def trainnetwork2():
    for user in friends1:
        friends2[user]=friends1[user]
        for friend in friends1[user]:
            friends2[user]=friends2[user].union(friends1[friend])
    print('Network2 trained!')

#Classify all transactions as trusted or unverified based on presence in a network of friends of degrees 1,2, and 4
def checkstream2(streamfile,outfile1,outfile2,outfile3):
    o1=open(outfile1,'w')
    o2=open(outfile2,'w')
    o3=open(outfile3,'w')

    t1=0
    t2=0
    t3=0
    u1=0
    u2=0
    u3=0
    
    with codecs.open(streamfile,'r','utf-8') as f:
        headers=f.readline().split(',')
        for line in f:
            payment=line.split(',')
            if len(payment) < 5:
                #make sure there are at least 5 fields in the payment, can be more than 5 because of commas in the comment
                print("unverified",file=o1)
                print("unverified",file=o2)
                print("unverified",file=o3)
                u1+=1
                u2+=1
                u3+=1
            else:
                try:
                    id1=int(payment[1])
                    id2=int(payment[2])
                    time1=datetime.datetime.strptime(payment[0],"%Y-%m-%d %H:%M:%S").time()
                    #check whether the first three fields are date, integer, integer
                except:
                    print("unverified",file=o1)
                    print("unverified",file=o2)
                    print("unverified",file=o3)
                    u1+=1
                    u2+=1
                    u3+=1
                    continue
                    
                if id1 in friends1:
                    if id2 in friends1[id1]:
                        print("trusted",file=o1)
                        t1+=1
                        print("trusted",file=o2)
                        print("trusted",file=o3)
                        t2+=1
                        t3+=1
                    else:
                        print("unverified",file=o1)
                        u1+=1
                        if id2 in friends2[id1]:
                            print("trusted",file=o2)
                            print("trusted",file=o3)
                            t2+=1
                            t3+=1
                        else:
                            print("unverified",file=o2)
                            u2+=1
                            #use the range 2 network a second time to find degree 4 friends, much faster than using the degree 1 network 4 times
                            for friend in friends2[id1]:
                                if id2 in friends2[friend]:
                                    print("trusted",file=o3)
                                    t3+=1
                                    break
                            else:
                                print("unverified",file=o3)
                                u3+=1
                else:
                    print("unverified",file=o1)
                    print("unverified",file=o2)
                    print("unverified",file=o3)
                    u1+=1
                    u2+=1
                    u3+=1

    f.close()
    o1.close()
    o2.close()
    o3.close()
    print("Output 1 trusted: %d untrusted: %d" % (t1,u1))
    print("Output 2 trusted: %d untrusted: %d" % (t2,u2))
    print("Output 3 trusted: %d untrusted: %d" % (t3,u3))

#memory saving version that doesn't use the degree 2 network, much longer run time
def checkstream1(streamfile,outfile1,outfile2,outfile3):
    o1=open(outfile1,'w')
    o2=open(outfile2,'w')
    o3=open(outfile3,'w')
    t1=0
    t2=0
    t3=0
    u1=0
    u2=0
    u3=0

    with codecs.open(streamfile,'r','utf-8') as f:
        headers=f.readline().split(',')
        for line in f:
            payment=line.split(',')
            if len(payment) < 5:
                print("unverified",file=o1)
                print("unverified",file=o2)
                print("unverified",file=o3)
                u1+=1
                u2+=1
                u3+=1
            else:
                id1=payment[1]
                id2=payment[2]
                if id1 not in friends1:
                    print("unverified",file=o1)
                    print("unverified",file=o2)
                    print("unverified",file=o3)
                    u1+=1
                    u2+=1
                    u3+=1
                else:
                    if id2 in friends1[id1]:
                        print("trusted",file=o1)
                        print("trusted",file=o2)
                        print("trusted",file=o3)
                        t1+=1
                        t2+=1
                        t3+=1
                        continue
                    else:
                        print("unverified",file=o1)
                        checked2=set([id1])
                        for friend in friends1[id1]:
                            if id2 in friends1[friend]:
                                print("trusted",file=o2)
                                print("trusted",file=o3)
                                t2+=1
                                t3+=1
                                break
                            else:
                                checked2=checked2.union(friends1[friend])
                        else:
                            print("unverified",file=o2)
                            u2+=1
                            checked3=set()
                            for friend2 in checked2:
                                if id2 in friends1[friend2]:
                                    print("trusted",file=o3)
                                    t3+=1
                                    break
                                else:
                                    checked3=checked3.union(friends1[friend2])
                            else:
                                print("unverified",file=o3)
                                u3+=1
                        #finding range 3 friends with the range 1 network already takes longer than finding range 4 friends with the range 2 network
    f.close()
    o1.close()
    o2.close()
    o3.close()
    print("Output 1 trusted: %d untrusted: %d" % (t1,u1))
    print("Output 2 trusted: %d untrusted: %d" % (t2,u2))
    print("Output 3 trusted: %d untrusted: %d" % (t3,u3))

#allow new users into the network but only if they make a payment, not receive
def bonus(streamfile,outfile4):
    o4=open(outfile4,'w')

    t4=0
    u4=0
    n4=0
    
    with codecs.open(streamfile,'r','utf-8') as f:
        headers=f.readline().split(',')
        for line in f:
            payment=line.split(',')
            if len(payment) < 5:
     #           print ('Malformed stream payment: ' + line)
                print("unverified",file=o4)
                u4+=1
            else:
                try:
                    id1=int(payment[1])
                    id2=int(payment[2])
                    time1=datetime.datetime.strptime(payment[0],"%Y-%m-%d %H:%M:%S").time()
                except:
                    print("unverified",file=o4)
                    u4+=1
                    continue
                    
                if id1 in friends1:
                    if id2 in friends1[id1]:
                        print("trusted",file=o4)
                        t4+=1
                    else:
                        if id2 in friends2[id1]:
                            print("trusted",file=o4)
                            t4+=1
                        else:
                            for friend in friends2[id1]:
                                if id2 in friends2[friend]:
                                    print("trusted",file=o4)
                                    t4+=1
                                    break
                            else:
                                print("unverified",file=o4)
                                u4+=1
                else:
                    if id2 in friends1:
                        print("new sender",file=o4)
                        n4+=1
                        friends1[id1]=set([id2])                        
                        friends2[id1]=set([id2]).union(friends1[id2])
                        for friend in friends1[id2]:
                            friends2[friend].add(id1)
                        friends1[id2].add(id1)        
    f.close()
    o4.close()
    print("Output 4 trusted: %d untrusted: %d new: %d" % (t4,u4,n4))
    
def main():
    args = sys.argv[1:]
    if len(args) < 5:
        print ('usage: payfile streamfile output1 output2 output3')
        sys.exit(1)
    payfile=args[0]
    streamfile=args[1]
    outfile1=args[2]
    outfile2=args[3]
    outfile3=args[4]

#    start=timeit.default_timer()
    trainnetwork1(payfile)
#    checkstream1('../paymo_input/stream_payment.csv','../paymo_output/output1.txt','../paymo_output/output2.txt','../paymo_output/output3.txt')
    trainnetwork2()
    checkstream2(streamfile,outfile1,outfile2,outfile3)
    if len(args)==6:        
        bonus(streamfile,outfile4)
#    stop=timeit.default_timer()
#    print (stop-start)    


if __name__ == '__main__':
  main()

