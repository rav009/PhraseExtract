#!/usr/bin/env python
# -*- coding: utf-8 -*-


def log(err):
    with open("/tmp/phrase_mapper.log", "a+") as f:
        f.write(str(err) + "\n")


try:
    log("[Info] Begin to import libraries")
    import sys
    import re
    import uuid
    import nltk
    import getopt
except Exception as err:
    log(err)

stop_words = [u'mailto', u'i', u'me', u'my', u'myself', u'we', u'our', u'ours', u'ourselves', u'you', u"you're",
              u"you've", u"you'll", u"you'd", u'your', u'yours', u'yourself', u'yourselves', u'he', u'him', u'his',
              u'himself', u'she', u"she's", u'her', u'hers', u'herself', u'it', u"it's", u'its', u'itself', u'they',
              u'them', u'their', u'theirs', u'themselves', u'what', u'which', u'who', u'whom', u'this', u'that',
              u"that'll", u'these', u'those', u'am', u'is', u'are', u'was', u'were', u'be', u'been', u'being', u'have',
              u'has', u'had', u'having', u'do', u'does', u'did', u'doing', u'a', u'an', u'the', u'and', u'but', u'if',
              u'or', u'because', u'as', u'until', u'while', u'of', u'at', u'by', u'for', u'with', u'about', u'against',
              u'between', u'into', u'through', u'during', u'before', u'after', u'above', u'below', u'to', u'from',
              u'down', u'in', u'out', u'on', u'off', u'over', u'under', u'again', u'further', u'then', u'once', u'here',
              u'there', u'when', u'where', u'why', u'how', u'all', u'any', u'both', u'each', u'few', u'more', u'most',
              u'other', u'some', u'such', u'no', u'nor', u'not', u'only', u'own', u'same', u'so', u'than', u'too',
              u'very', u's', u't', u'can', u'will', u'just', u'don', u"don't", u'should', u"should've", u'now', u'd',
              u'll', u'm', u'o', u're', u've', u'y', u'ain', u'aren', u"aren't", u'couldn', u"couldn't", u'didn',
              u"didn't", u'doesn', u"doesn't", u'hadn', u"hadn't", u'hasn', u"hasn't", u'haven', u"haven't", u'isn',
              u"isn't", u'ma', u'mightn', u"mightn't", u'mustn', u"mustn't", u'needn', u"needn't", u'shan', u"shan't",
              u'shouldn', u"shouldn't", u'wasn', u"wasn't", u'weren', u"weren't", u'won', u"won't", u'wouldn',
              u"wouldn't"]


def genuuid():
    return str(uuid.uuid4()).replace('-', '')


def hideemail(s):
    edict = {}
    emailregex = r"[-_\w\.]{0,64}@[-\w]{1,63}\.*[-\w]{1,63}"
    es = re.findall(emailregex, s)
    if len(es) > 0:
        for e in es:
            if e not in edict.values():
                u = genuuid()
                s = s.replace(e, u)
                edict[u] = e
    return s, edict


def hidehyphen(s):
    hdict = {}
    hyphenregex = r"[\w]+[\-]{1,}[\w\-]*[\w]+]*"
    hs = re.findall(hyphenregex, s)
    if len(hs) > 0:
        for h in hs:
            if h not in hdict.values():
                u = genuuid()
                s = s.replace(h, u)
                hdict[u] = h
    return s, hdict


def hideip(s):
    ipdict = {}
    ipregex = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
    ips = re.findall(ipregex, s)
    if len(ips) > 0:
        for ip in ips:
            if ip not in ipdict.values():
                u = genuuid()
                s = s.replace(ip, u)
                ipdict[u] = ip
    return s, ipdict


def hideurl(s):
    udict = {}
    urlregex = r"((http|https|ftp)\://([a-zA-Z0-9\.\-]+(\:[a-zA-Z0-9\.&amp;%\$\-]+)*@)*((25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9])\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[0-9])|localhost|([a-zA-Z0-9\-]+\.)*[a-zA-Z0-9\-]+\.(com|edu|gov|int|mil|net|org|biz|arpa|info|name|pro|aero|coop|museum|[a-zA-Z]{2}))(\:[0-9]+)*(/($|[a-zA-Z0-9\.\,\?\'\\\+&amp;%\$#\=~_\-]+))*)"
    urls = re.findall(urlregex, s)
    if len(urls) > 0:
        for url in urls:
            if url[0] not in udict.values():
                u = genuuid()
                s = s.replace(url[0], u)
                udict[u] = url[0]
    shorturlregex = r"(www\.[a-zA-Z].[a-zA-Z0-9\-]*\.(com|edu|gov|mil|net|org|biz|info|name|museum|us|ca|uk)[\S]*)"
    surls = re.findall(shorturlregex, s)
    if len(surls) > 0:
        for surl in surls:
            if surl[0] not in udict.values():
                u = genuuid()
                s = s.replace(surl[0], u)
                udict[u] = surl[0]
    return s, udict


def restorehiddenword(w, d):
    if w in d.keys():
        return d[w]
    return w


def notdigits(s):
    for i in s:
        if not str(i).isdigit():
            return True
    return False


def read_stopsentences():
    rs = []
    with open('whole', 'r') as f:
        rs = f.readlines()
    return rs


def tokenize(s):
    s = s.lower()
    s = s.replace("\\n", " ")

    otherwordsregex = r"[^\s\w:\+\-,\?\{\}\[\]\>\<@\$\.\(\)\#/\|'\"!&*;=~%\^]+"
    s = re.sub(otherwordsregex, ' ', s)

    s = s.replace('&amp;', ' ')
    s = s.replace('&gt;', ' ')
    s = s.replace('&lt;', ' ')
    s = s.replace(';', ' ')
    s = s.replace('+', ' ')
    s = s.replace('<', ' ')
    s = s.replace('>', ' ')
    s = s.replace(':', ' ')
    s = s.replace('[', ' ')
    s = s.replace(']', ' ')
    s = s.replace('{', ' ')
    s = s.replace('}', ' ')
    s = s.replace('(', ' ')
    s = s.replace(')', ' ')
    # s = s.replace('/', ' ')
    s = s.replace('?', ' ')
    s = s.replace('*', ' ')
    s = s.replace(',', ' ')
    s = s.replace('\'s', ' ')
    s = s.replace('\'', ' ')
    s = s.replace('"', ' ')
    s = s.replace('|', ' ')
    s = s.replace('#', ' ')
    s = s.replace('__', ' ')
    s = s.replace(' _ ', ' ')
    s = s.replace(' = ', ' ')
    s = s.replace('!', ' ')
    s = s.replace('@', ' ')

    # recognize&hide email
    s, edict = hideemail(s)

    # recognize hyphen
    s, hdict = hidehyphen(s)

    # recognize ip
    s, ipdict = hideip(s)

    # recognize url
    s, udict = hideurl(s)

    d = edict.copy()
    d.update(hdict)
    d.update(ipdict)
    d.update(udict)

    s = s.replace('-', ' ')
    s = s.replace('.', ' ')
    s = s.replace('&', ' ')

    from nltk.tokenize import word_tokenize
    rs = [restorehiddenword(w, d) for w in word_tokenize(s)]

    return [w for w in rs if w not in stop_words]


if __name__ == "__main__":
    try:
        stop_sentences = read_stopsentences()
        nltk.data.path.append("/home/rav009/nltk_data/")
        phrase_len = 3
        opts, args = getopt.getopt(sys.argv[1:], "l:")
        for c, v in opts:
            if c == "-l":
                phrase_len = int(v)
        for line in sys.stdin:
            begin = line.find("|")
            casenumber = line[0:begin]
            line = line[begin:]
            for s in stop_sentences:
                line = line.replace(s.strip() + "\\n", " ")
            line = line.replace("\\n", " ")
            ows = tokenize(line)
            length = len(ows)
            for i in range(0, length):
                for j in range(2, phrase_len + 1):
                    if i + j <= length:
                        print ';'.join(ows[i:i + j]) + "\t" + casenumber
    except Exception as err:
        log(err)