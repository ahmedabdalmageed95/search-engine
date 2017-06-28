from html.parser import HTMLParser  
from urllib.request import urlopen  
from urllib import parse
from concurrent import futures
from urllib import robotparser
import threading
import sys
# We are going to create a class called LinkParser that inherits some
# methods from HTMLParser which is why it is passed into the definition
class LinkParser(HTMLParser):
	visited = []
	numVisited = 0
	# This is a function that HTMLParser normally has
	# but we are adding some functionality to it
	def handle_starttag(self, tag, attrs):
		# We are looking for the begining of a link. Links normally look
		# like <a href="www.someurl.com"></a>
		if tag == 'a':
			for (key, value) in attrs:
				if key =='href':
					# We are grabbing the new URL. We are also adding the
					# base URL to it. For example:
					# www.netinstructions.com is the base and
					# somepage.html is the new URL (a relative URL)
					#
					# We combine a relative URL with the base URL to create
					newUrl = parse.urljoin(self.baseUrl, value)
					# And add it to our colection of links:
					self.links = self.links + [newUrl]

	# This is a new function that we are creating to get links
	# that our spider() function will call
	def getLinks(self, url):
		self.links = []
		# Remember the base URL which will be important when creating
		# absolute URLs
		self.baseUrl = url
		# Use the urlopen function from the standard Python 3 library
		response = urlopen(url)
		# Make sure that we are looking at HTML and not other things that
		# are floating around on the internet (such as
		# JavaScript files, CSS, or .PDFs for example)
		if response.getheader('Content-Type')=='text/html':
			htmlBytes = response.read()
			# Note that feed() handles Strings well, but not bytes
			# (A change from Python 2.x to Python 3.x)
			htmlString = htmlBytes.decode("utf-8")
			self.feed(htmlString)
			return htmlString, self.links
		else:
			return "",[]

# And finally here is our spider. It takes in an URL, a word to find,
# and the number of pages to search through before giving up
def spider(url,superMaxPages):
	lock = threading.Lock()
	writeLock = threading.Lock()
	mydata = threading.local()
	mydata.numberVisited = 0  
	mydata.pagesToVisit = url

	while mydata.pagesToVisit != [] and LinkParser.numVisited < int(superMaxPages):

		mydata.numberVisited = 0

		# Start from the beginning of our collection of pages to visit:
		url = mydata.pagesToVisit[0]
		if '.org' in url:
			maxPages = 8
		elif '.edu' in url:
			maxPages = 5
		else:
			maxPages = 4

# frequency of visitingg the URL, we chose it to be based on the domain (to be easier for us)
		while mydata.pagesToVisit != [] and mydata.numberVisited < int(maxPages):
# Start from the beginning of our collection of pages to visit:
			url = mydata.pagesToVisit[0]
# In case we are not allowed to read the page -> delete url -> continue.
			rp = robotparser.RobotFileParser()
			rp.set_url(url+'/robots.txt')
			rp.read()
			if not(rp.can_fetch("*", url)):
				del mydata.pagesToVisit[0]
				continue

# lock the vistied list, since more than one thread reads and writes to it.
			lock.acquire()
			try:
				if url in LinkParser.visited and mydata.pagesToVisit != []:
					del mydata.pagesToVisit[0]
					print("cotinue")
					continue
				else:
					LinkParser.visited.append(url)
			finally:
				lock.release() # this won't block
			mydata.numberVisited += 1
			LinkParser.numVisited += 1

			writeLock.acquire()
			f.write(url+'\n')
			writeLock.release()

			mydata.pagesToVisit = mydata.pagesToVisit[1:]
			print(LinkParser.numVisited)
			print(LinkParser.visited[0:LinkParser.numVisited])
			try:
				parser = LinkParser()
				data, links = parser.getLinks(url)
				# Add the pages that we visited to the end of our collection
				# of pages to visit:
				mydata.pagesToVisit = mydata.pagesToVisit + links
				print("One more page added from &i",threading.get_ident())
			except:
				print(" **Failed!**")

class myThread (threading.Thread):
	def __init__(self, url, maxPages):
		threading.Thread.__init__(self)
		self.maxPages = maxPages
	def run(self):
		spider (url, maxPages)

def file_len(fname):
	with open(fname) as f:
		return(sum(1 for _ in f))

if __name__ == '__main__':

# Retirve previously made list.
	length = file_len("urls.txt")

	# Important: this is to be changed !!!
	######################################
	f = open('urls.txt','w')			
	######################################
	for x in range(length):
		LinkParser.visited[x].append(f.readline())

	threads = []
	url = []
	numberOfThreads = sys.argv[3]
	url.append("https://docs.python.org/3/py-modindex.html")
	# url.append("https://docs.python.org/3/py-modindex.html")
	# url.append("https://docs.python.org/3/library/threading.html")

	maxPages = sys.argv[2]	# This value is overriden in the function Spider #
	for i in range(1,int(numberOfThreads)):
		threads.append(myThread( spider, (url,maxPages) ))	# This value is overriden in the function Spider #

	print("Number of created threads is ", threading.active_count(), "threads len is" , len(threads))

	for i in range(1,int(numberOfThreads)):
		threads[i-1].start()

	print("Number of created threads is ", threading.active_count(), "threads len is" , len(threads))

	for j in range(1,int(numberOfThreads)):
		threads[j-1].join()

	# This zero should be overriden after we know the number of acual urls crawled.
	f.write(str(LinkParser.numVisited))		# This '0' means the Indexer was here, -the indexer sets the '0'-,	// Fares.
										# if it is 'x', then, 'x' pages have been added to the list, since last time the indexer has indexed.
										# This is done to save the indexer from re indexing indexed pages.
	f.close()
