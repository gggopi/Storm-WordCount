import storm
class SplitSentenceBolt(storm.BasicBolt):
    def process(self,tup):
        #print 'qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq'
        #print tup.values[0]
        if tup.values:
            words = tup.values[0].split(" ")
            if words:
                for word in words:
                    storm.emit([word])
        else:
            pass
SplitSentenceBolt().run()