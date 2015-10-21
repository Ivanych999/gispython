# -*- coding: utf-8 -*-
__author__ = 'medvedev.ivan@mail.ru'

import os,sys,datetime,argparse,threading,time,shutil
from osgeo import gdal
from gdalconst import *
from Queue import Queue

queue = Queue()
LOCK = threading.RLock()

# CONSTs
folders = {
	0.5: '0_5', # subfolder for raster with 0.5 pixel size
	1.0: '1_0', # subfolder for raster with 1.0 pixel size
	1.5: '1_5', # subfolder for raster with 1.5 pixel size
	2.0: '2_0', # subfolder for raster with 2.0 pixel size
	2.5: '2_5'  # subfolder for raster with 2.5 pixel size
		   }

# severity: 0 - Info, 1 - Warning, 2 - Error
def AddMessage(severity,message):
	sd = {0: 'Info', 1: 'Warning', 2: 'Error'}
	print "{0} at {1}: {2}".format(sd[severity],datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S'),message)

def ParseArgs():
	ap = argparse.ArgumentParser(description='Reproject rasters and build pyramids')
	
	ap.add_argument('--in-folder', '-i', type=str, action='store', required=True, help='Root folder for scaning GeoTIFF files')
	ap.add_argument('--tmp-folder', '-t', type=str, action='store', required=False, help='Temp folder')
	ap.add_argument('--out-folder', '-o', type=str, action='store', required=True, help='CSV file name')
	ap.add_argument('--epsg', '-e', type=int, action='store', default=3857, required=True, help='Target EPSG')
	ap.add_argument('--threding-count', '-c', type=int, action='store', default=24, required=True, help='Threading count')
	ap.add_argument('--replace', '-r', action='store_true', default=False, help='Replace output')
	ap.add_argument('--build', '-b', action='store_true', default=False, help='Build pyramids')
	
	args = ap.parse_args()

	args_dict = vars(args)
	
	return vars(args)

def BuildPyramids(rst):
	try:
		if os.path.exists(rst):
			if os.path.exists(rst+'.ovr'):
				os.remove(rst+'.ovr')
			return os.system('gdaladdo -ro "%s" 2 4 8 16 32' % rst)
	except Exception,err:
		return u'Error: %s' % unicode(err)

def Reproject(i_rst, o_rst, t_epsg):
	try:
		if os.path.exists(i_rst):
			if os.path.exists(o_rst):
				os.remove(o_rst)
			return os.system('gdalwarp -t_srs EPSG:%s "%s" "%s"' % (t_epsg,i_rst,o_rst))
	except Exception,err:
		return u'Error: %s' % unicode(err)

def SaveStatFirstLine(csv, use_temp = True):
	csvf = open(csv,'w')
	if use_temp:
		csvf.write('"src_file";"result";"dst_folder";"dst_file";"dst_file_size";"reproject_time";"pyramids_time";"pyramids_size";"moving_time"\n')
	else:
		csvf.write('"src_file";"result";"dst_folder";"dst_file";"dst_file_size";"reproject_time";"pyramids_time";"pyramids_size"\n')
	csvf.close()

def SaveStatLine(csv, line, use_temp = True):
	global LOCK

	LOCK.acquire()
	csvf = open(csv,'a')
	if use_temp:
		csvf.write('"{0}";"{1}";"{2}";"{3}";{4};"{5}";"{6}";{7};"{8}"\n'.format(line.get('src_file',''),line.get('result',''),line.get('dst_folder',''),line.get('dst_file',''),line.get('dst_file_size',0),line.get('reproject_time',''),line.get('pyramids_time',''),line.get('pyramids_size',0),line.get('moving_time','')))
	else:
		csvf.write('"{0}";"{1}";"{2}";"{3}";{4};"{5}";"{6}";{7}\n'.format(line.get('src_file',''),line.get('result',''),line.get('dst_folder',''),line.get('dst_file',''),line.get('dst_file_size',0),line.get('reproject_time',''),line.get('pyramids_time',''),line.get('pyramids_size',0)))
	csvf.close()
	LOCK.release()

def doWork():
	global queue
	global use_temp

	while True:
		# Try get task from queue
		try:
			c_task = queue.get_nowait()
		except:
			return

			# Initialize stat dict
		stat_line = {}

		# Set start stat parameters
		stat_line['src_file'] = c_task['in_file']
		stat_line['result'] = 'success'
		stat_line['dst_folder'] = c_task['out_folder']
		stat_line['dst_file'] = os.path.basename(c_task['in_file'])

		AddMessage(0,'Processes with %s' % c_task['in_file'])
		try:
			tmp_name = os.path.join(c_task['out_folder'],stat_line['dst_file'])
			if use_temp:
				tmp_name = os.path.join(c_task['tmp_folder'],stat_line['dst_file'])
		
			# Start reproject
			p_start_time = datetime.datetime.now()
			Reproject(c_task['in_file'],tmp_name,c_task['epsg'])
			stat_line['dst_file_size'] = os.path.getsize(tmp_name)
			stat_line['reproject_time'] = '%s' % (datetime.datetime.now()-p_start_time)

			if c_task['build']:
				# Start build pyramids
				pm_start_time = datetime.datetime.now()
				BuildPyramids(tmp_name)
				stat_line['pyramids_size'] = os.path.getsize(tmp_name+'.ovr')
				stat_line['pyramids_time'] = '%s' % (datetime.datetime.now()-pm_start_time)

			if use_temp:
				# Start moving results
				mv_start_time = datetime.datetime.now()
				out_name = os.path.join(c_task['out_folder'],stat_line['dst_file'])
				shutil.move(tmp_name,out_name)
				if c_task['build']:
					shutil.move(tmp_name+'.ovr',out_name+'.ovr')
				stat_line['moving_time'] = '%s' % (datetime.datetime.now()-mv_start_time)
		except Exception,err:
			AddMessage(2,'Cannot process file %s' % c_task['in_file'])
			stat_line['result'] = 'error'
			stat_line['dst_folder'] = err

		SaveStatLine(c_task['stat_file'],stat_line,use_temp)

def main():
	global queue
	global use_temp

	# Parsing input args
	args = ParseArgs()

	# Check input folder
	if os.path.exists(args['in_folder']):

		# Check output folder
		if not os.path.exists(args['out_folder']):
			try:
				os.mkdir(args['out_folder'])
				AddMessage(0,'Folder %s created' % args['out_folder'])
			except Exception,err:
				AddMessage(2,'Cannot create output folder %s: %s' % (args['out_folder'],err))
				return

		# Set use temp

		use_temp = False
		if args.get('tmp_folder',None):
			use_temp = True

		# Check temp folder
		if use_temp:
			if not os.path.exists(args['tmp_folder']):
				try:
					os.mkdir(args['tmp_folder'])
					AddMessage(0,'Folder %s created' % args['tmp_folder'])
				except Exception,err:
					AddMessage(2,'Cannot create temp folder %s: %s' % (args['tmp_folder'],err))
					return

		AddMessage(0,'Start scan %s' % args['in_folder'])

		# Create statistic file
		csv = os.path.join(args['out_folder'],'statistic.csv')
		if use_temp:
			csv = os.path.join(args['tmp_folder'],'statistic.csv')
		SaveStatFirstLine(csv,use_temp)

		# Start scan folders tree and making queue
		for root,dir,files in os.walk(args['in_folder']):
			AddMessage(0,'Current dir is %s' % root)
			for f in files:
				if f.rpartition('.')[2].lower() == 'tif':

					f_task = {}
					f_task['in_file'] = os.path.join(root,f)
					f_task['tmp_folder'] =  args.get('tmp_folder',None)
					f_task['build'] = args['build']
					f_task['stat_file'] = csv
					f_task['epsg'] = args['epsg']
					f_task['replace'] = args['replace']

					AddMessage(0,'Processes with %s ' % f)
					try:
						ro = gdal.Open(f_task['in_file'], GA_ReadOnly)
						if ro:
							f_task['out_folder'] = os.path.join(args['out_folder'],folders[ro.GetGeoTransform()[1]])
							ro = None

							# Check output folder
							if not os.path.exists(f_task['out_folder']):
								try:
									os.mkdir(f_task['out_folder'])
									AddMessage(0,'Folder %s created' % f_task['out_folder'])
								except Exception,err:
									AddMessage(2,'Cannot create output folder %s: %s' % (args['out_folder'],err))
									continue

							if os.path.exists(os.path.join(f_task['out_folder'],f)):
								if args['replace']:
									AddMessage(1,'Start removing output file %s' % f)
									os.remove(os.path.join(f_task['out_folder'],f))
									if os.path.exists(os.path.join(f_task['out_folder'],f)+'.ovr'):
										os.remove(os.path.join(f_task['out_folder'],f)+'.ovr')
									AddMessage(0,'Output file %s removed' % f)
									queue.put(f_task)
									AddMessage(0,'File %s put into queue' % f)
							else:
								queue.put(f_task)
								AddMessage(0,'File %s put into queue' % f)
						else:
							AddMessage(1,'Error while opening file %s' % f)
					except:
						AddMessage(1,'Error while opening file %s' % f)
		AddMessage(0,'Scaning completed')
		AddMessage(0,'Start work with tasks')

		for _ in xrange(args['threding_count']):
			thread_ = threading.Thread(target=doWork)
			thread_.start()

		while threading.active_count() > 1:
			time.sleep(1)

		AddMessage(0,'All tasks completed')

if __name__ == '__main__':
	main()
