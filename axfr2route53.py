#!/usr/bin/env python

# Take an AXFR on stdin and create the zone with all its elements (except
# NS) as a Route53 hosted zone

from optparse import OptionParser
from boto import route53
import sys , os , re , time

# Put in Access Key
AK = ''
# Secret Key
SK = ''

def getOpts():
    usage = 'Usage: %prog [options] domain_name'
    desc = 'This program takes an AXFR transfer on stdin and creates a ' \
        'Route53 hosted zone.  Essentially, you should "dig @ns-server ' \
        'domain axfr | %s example.com"' % __file__
    p = OptionParser(usage=usage , description=desc)
    p.add_option('-a' , '--access-key' , meta='STR' , dest='ak' , default=AK ,
        help='REQUIRED: Your amazon account (or IAM user) access key '
        '[default: %default]')
    p.add_option('-s' , '--secret-key' , meta='STR' , dest='sk' , default=SK ,
        help='REQUIRED: Your amazon account (or IAM user) secret key ' 
        '[default: %default]')
    opts , args = p.parse_args()
    if not opts.ak:
        p.error('You MUST have an access key')
    if not opts.sk:
        p.error('You MUST have a secret key')
    return (opts , args)

def parseStdin(opts , zone , conn):
    ignRe = re.compile(r'^\s*(;.*|\s*)$')
    recRe = re.compile(r'^(?P<name>\S+)\s+(?P<ttl>\S+)\s+(?P<class>\S+)\s+' \
        r'(?P<type>\S+)\s+(?P<rr>.*)$')
    rrset = route53.record.ResourceRecordSets(conn , zone.id)
    records = {}
    for line in sys.stdin:
        if ignRe.match(line): continue
        m = recRe.match(line)
        if not m:
            print >> sys.stderr , 'Error matching line, skipping: %s' % line
            continue
        if m.group('type').upper() in ('NS' , 'SOA'):
            # skip NS and SOA records as they are created automatically
            continue
        key = '%s%s' % (m.group('name') , m.group('type'))
        if key not in records:
            records[key] = {}
        records[key]['name'] = m.group('name')
        records[key]['ttl'] = int(m.group('ttl'))
        records[key]['class'] = m.group('class')
        records[key]['type'] = m.group('type')
        if 'rrs' not in records[key]:
            records[key]['rrs'] = []
        records[key]['rrs'].append(m.group('rr'))
    for r in records.itervalues():
        rrset.add_change('CREATE' , r['name'] , r['type'] , r['ttl'] , 
            resource_records=r['rrs'])
    return rrset

def createZone(zone , opts , conn):
    zones = conn.get_all_hosted_zones()
    resp = zones['ListHostedZonesResponse']
    if 'HostedZones' in resp and zone in [z['Name'].strip('.') for z in 
            resp['HostedZones']]:
        print >> sys.stderr , \
            'Warning! Zone %s already exists! Using existing' % zone
        return conn.get_zone(zone)
    return conn.create_zone(zone)

def main():
    opts , args = getOpts()
    conn = route53.connection.Route53Connection(opts.ak , opts.sk)
    try:
        zone = createZone(args[0] , opts , conn)
    except Exception , e:
        print >> sys.stderr , 'An error occurred creating the zone %s: %s' % \
            (args[0] , e)
        sys.exit(1)
    if not zone:
        print >> sys.stderr , 'Could not create or find zone %s' % args[0]
        sys.exit(2)
    rrset = parseStdin(opts , zone , conn)
    resp = rrset.commit()
    chid = resp['ChangeResourceRecordSetsResponse']['ChangeInfo']\
        ['Id'].split('/')[-1]
    status = resp['ChangeResourceRecordSetsResponse']['ChangeInfo']['Status']
    print status
    while status != 'INSYNC':
        time.sleep(10)
        ch = conn.get_change(chid)
        status = ch['GetChangeResponse']['ChangeInfo']['Status']
        print status

if __name__ == '__main__':
    main()
