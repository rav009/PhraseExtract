# -*- coding: utf-8 -*-

import sys
import re
import uuid
import nltk
import getopt


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
              u"wouldn't", u"up", u"e", u"c", u"yes", u"good", u"february", u"2017", u"exe", u"thanks", u"september",

              u"help", u"tuesday", u"pm", u"friday", u"ok", u"salesforce", u"thank", u"questions", u"like", u"seems",
              u"sorry", u"please", u"via", u"iphone", u"email", u"thursday", u"wednesday", u"month", u"week", u"europe",
              u"monday", u"sent", u"best", u"hi", u"okay", u"let", u"would", u"e-mail", u"http", u"get", u"per",
              u"elekta", u"support", u"uk", u"ip", u"march", u"england"]


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
    try:
        with open('whole', 'r') as f:
            rs.append(f.readline())
    except Exception as e:
        print >> sys.stderr, str(e)
    return rs


def tokenize(s):
    s = s.lower()

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
    s = s.replace('/', ' ')
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

    try:
        from nltk.tokenize import word_tokenize
        rs = [restorehiddenword(w, d) for w in word_tokenize(s)]
    except Exception as e:
        print >> sys.stderr, "NLTK TOKENIZE ERROR:"+str(e)
        exit(-1)

    return [w for w in rs if w not in stop_words]


if __name__ == "__main__":
    stop_sentences = read_stopsentences()
    nltk.data.path.append("/home/rav009/nltk_data/")
    phrase_len = 3
    try:
        opts, args = getopt.getopt(sys.argv[1:], "l:")
        for c, v in opts:
            if c == "-l":
                phrase_len = int(v)
    except getopt.GetoptError:
        print "Command line arguments error."
        sys.exit(-2)

    for line in sys.stdin:
        for s in stop_sentences:
            line = line.replace(s, " ")
        ows = tokenize(line)
        ws = ows[1:]
        l = len(ws)
        for i in range(0, l):
            for j in range(2, phrase_len+1):
                if i+j <= l and notdigits(ws[i:i+j]):
                    print ';'.join(ws[i:i+j]) + "\t" + ows[0]