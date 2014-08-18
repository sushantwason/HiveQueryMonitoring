import alerts
import time


a=alerts.Process()
a.readProcessedJobs()
while True:
	a.getJobs()
	#exit()
	time.sleep(15)	


