from sgmllib import SGMLParser

class URLLister(SGMLParser):
    def reset(self):                              
        SGMLParser.reset(self)
        self.linksList = []

    def start_a(self, attrs):
        for k,v in attrs:
            if k == 'href':
              self.linksList.append(v) 
