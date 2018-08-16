# -*- coding: utf-8 -*-

import os
import sys
import requests
import xmltodict
from six.moves import queue as Queue
from threading import Thread
import re
import json
import datetime
import logging
import time


logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
handler = logging.FileHandler("log_bugging.log")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
console = logging.StreamHandler()
logger.addHandler(handler)
logger.addHandler(console)


# 设置请求超时时间
TIMEOUT = 30

# 尝试次数
RETRY = 5

# 分页请求的起始点
START = 0

# 每页请求个数
MEDIA_NUM = 50

# 并发线程数
THREADS = 10

# 是否下载图片
ISDOWNLOADIMG=True
ISDOWNLOADIMG= not ISDOWNLOADIMG

#是否下载视频
ISDOWNLOADVIDEO=True
# ISDOWNLOADVIDEO= not ISDOWNLOADVIDEO

#下载最近几天的数据
DOWN_LIMIT_DATE=datetime.datetime.today() - datetime.timedelta(days=14)



class DownloadWorker(Thread):
    def __init__(self, queue, proxies=None):
        Thread.__init__(self)
        self.queue = queue
        self.proxies = proxies

    def run(self):
        while True:
            medium_type, post, target_folder,caption = self.queue.get()
            self.download(medium_type, post, target_folder,caption)
            self.queue.task_done()

    def download(self, medium_type, post, target_folder,caption):
        try:
            medium_url ,hd_type= self._handle_medium_url(medium_type, post)
            if medium_url is not None:
                # self._download(medium_type, medium_url, target_folder,caption)
                self._downloadStream(medium_type, medium_url, hd_type, target_folder,caption)
        except TypeError:
            pass

    def _handle_medium_url(self, medium_type, post):
        try:
            if medium_type == "photo":				
                return post["photo-url"][0]["#text"]
            if medium_type == "video":
                video_player = post["video-player"][1]["#text"]
                hd_pattern = re.compile(r'.*"hdUrl":("([^\s,]*)"|false),')
                hd_match = hd_pattern.match(video_player)
                # logger.info("get hd_match   : %s    =====================",video_player)
                try:
                    if hd_match is not None and hd_match.group(1) != 'false':
                        return hd_match.group(2).replace('\\', ''),"HD"
                except IndexError:
                    pass
                pattern = re.compile(r'.*src="(\S*)" ', re.DOTALL)
                match = pattern.match(video_player), "NORMAL"
                if match is not None:
                    try:
                        return match.group(1)
                    except IndexError:
                        return None
        except:
            raise TypeError("找不到正确的下载URL "
                            "请到 "
                            "https://github.com/xuanhun/tumblr-crawler"
                            "提交错误信息:\n\n"
                            "%s" % post)

    def _download(self, medium_type, medium_url, target_folder,caption):
        medium_name = medium_url.split("/")[-1].split("?")[0]
        if medium_type == "video":
            if not medium_name.startswith("tumblr"):
                medium_name = "_".join([medium_url.split("/")[-2],
                                        medium_name])
         
            medium_name += ".mp4"
        elif medium_type=="photo":
            medium_name = medium_name[-14:]
        medium_name = caption+"_"+medium_name
        file_path = os.path.join(target_folder, medium_name)
        if not os.path.isfile(file_path):
            logger.info("Downloading %s from %s.\n" % (medium_name, medium_url))
            retry_times = 0
            while retry_times < RETRY:
                try:
                    resp = requests.get(medium_url,
                                        stream=True,
                                        proxies=self.proxies,
                                        timeout=TIMEOUT)
                    with open(file_path, 'wb') as fh:
                        for chunk in resp.iter_content(chunk_size=1024):
                            fh.write(chunk)
                    break
                except Exception as e :
                    # try again
                    logger.info(e )			
                    pass
                retry_times += 1
            else:
                try:
                    os.remove(file_path)
                except Exception:
                    logger.info(e )					
                    pass
                logger.info("Failed to retrieve %s from %s.\n" % (medium_type,medium_url))
               
               
               
    def _downloadStream(self, medium_type, medium_url, hd_type, target_folder,caption):
    
        # 获取当前文件大小
        def get_local_file_exists_size(local_path):
            try:
                lsize = os.stat(local_path).st_size
            except Exception as e:
                # print(e)
                lsize = 0
            return lsize    
            
        # 流式下载文件 
        def get_file_obj(down_link, offset):
            webPage = None
            try:
                headers = {'Range': 'bytes=%d-' % offset}
                # logger.info(headers)
                webPage = requests.get(down_link, stream=True, headers=headers, timeout=TIMEOUT,proxies=self.proxies, verify=False)
                status_code = webPage.status_code
                if status_code in [200, 206]:
                    webPage = webPage
                elif status_code == 416:
                    logger.info("文件数据请求区间错误 : %s , status_code : %s ",down_link, status_code)
                else:
                    logger.info("链接有误: %s ，status_code ：%s", down_link, status_code)
            except Exception as e:
                logger.info("无法链接 ：%s , exception : %s",down_link, e)
            finally:
                return webPage        
                
        # logger.info("get %s    =====================",medium_url)
        try :
            medium_name = medium_url.split("/")[-1].split("?")[0]
            if medium_type == "video":
                if not medium_name.startswith("tumblr"):
                    medium_name = "_".join([medium_url.split("/")[-2],
                                            medium_name])
             
                medium_name += ".mp4"
                
                # 临时修改，手动程跳转地址
                # print("hd_type :"+hd_type+"    medium_url :"+medium_url)
                if (hd_type=="HD"):
                    medium_url = "https://vtt.tumblr.com/"+medium_url.split("/")[-2]+"_"+medium_url.split("/")[-1]+".mp4"
                else:
                    medium_url = "https://vtt.tumblr.com/"+medium_url.split("/")[-1]+".mp4"
                logger.info("Downloading hd_type %s , %s from %s.\n" % (hd_type, medium_name, medium_url))
            elif medium_type=="photo":
                medium_name = medium_name[-14:]
                
            medium_name = caption+"_"+medium_name
            
            medium_name = medium_name.replace(":","").replace("\n","")
            file_path = os.path.join(target_folder, medium_name)
        except Exception as e :
            print(e)

        file_size = -1
        get_file_size = -1
        retry_times = 0
        requests.packages.urllib3.disable_warnings()
        while get_file_size < 0 and retry_times < RETRY:
            try :
                r1 = requests.get(medium_url, stream=True, verify=False)
                file_size = int(r1.headers['Content-Length'])
                # file_size = file_size/1024
                get_file_size = 1
            except Exception as e :
                logger.info("medium_url %s , Get file size error" % medium_url)
                logger.info(e)
                file_size=-1
                retry_times += 1   
        
        lsize = get_local_file_exists_size(file_path)
        # print("file_size :"+file_size +"   lsize :"+lsize  )
        print("file_size : %s " % file_size   )
        if file_size != -1 and lsize != file_size:
            logger.info("Downloading %s  total size : %s  , from %s \n",medium_name,file_size,medium_url)
            retry_times = 0
            while retry_times < RETRY:
                try:
                    lsize = get_local_file_exists_size(file_path)
                    
                    if lsize == file_size:
                        break
                    webPage = get_file_obj(medium_url, lsize)
                    
                    try:
                        file_obj = open(file_path, 'ab+')
                    except Exception as e:
                        print(e)
                        logger.info("打开文件: %s 失败", file_path )
                        break
                    try:
                        for chunk in webPage.iter_content(chunk_size=10 *1024):
                            if chunk:
                                lsize = get_local_file_exists_size(file_path)
                                logger.info("Downloading %s  cur size : %s , total size : %s , from %s \n",file_path, lsize,file_size, medium_url)
                                file_obj.write(chunk)
                            else:
                                break
                    except Exception as e:
                        logger.info(e)
                        time.sleep(1)
                    file_obj.close()
                    webPage.close()
                except Exception as e :
                    # try again
                    logger.info(e)			
                    pass
                retry_times += 1
            else:
                # try:
                    # os.remove(file_path)
                # except Exception as e:
                    # logger.info(e )					
                    # pass
                logger.info("Failed to retrieve %s from %s.\n" % (medium_type,medium_url))                
    
            
class CrawlerScheduler(object):

    def __init__(self, sites, proxies=None):
        self.sites = sites
        self.proxies = proxies
        self.queue = Queue.Queue()
        self.scheduling()
      

    def scheduling(self):
        # 创建工作线程
        for x in range(THREADS):
            worker = DownloadWorker(self.queue,
                                    proxies=self.proxies)
            #设置daemon属性，保证主线程在任何情况下可以退出
            worker.daemon = True
            worker.start()

        for site in self.sites:
            if ISDOWNLOADIMG:
                self.download_photos(site)
            if ISDOWNLOADVIDEO:
                self.download_videos(site)
        

    def download_videos(self, site):
        self._download_media(site, "video", START)
        # 等待queue处理完一个用户的所有请求任务项
        self.queue.join()
        logger.info("视频下载完成 %s" % site)

    def download_photos(self, site):
        self._download_media(site, "photo", START)
         # 等待queue处理完一个用户的所有请求任务项
        self.queue.join()
        logger.info("图片下载完成 %s" % site)

    def _download_media(self, site, medium_type, start):
        current_folder = os.getcwd()
        target_folder = os.path.join(current_folder, site)
        if not os.path.isdir(target_folder):
            os.mkdir(target_folder)

        base_url = "http://{0}.tumblr.com/api/read?type={1}&num={2}&start={3}"
        start = START
        breakFlag=True
        while breakFlag:
            media_url = base_url.format(site, medium_type, MEDIA_NUM, start)
            logger.info(media_url)    
            response = requests.get(media_url,
                                    proxies=self.proxies)
            data = xmltodict.parse(response.content)
            try:
                posts = data["tumblr"]["posts"]["post"]
                for post in posts:					
                    # 文件说明
                    caption = ""					
                    if medium_type == "photo":	
                        if "photo-caption" in post:					
                            caption=post["photo-caption"]
                    else:
                        if "video-caption" in post:					
                            caption=post["video-caption"]						
                    htmlTagPattern = re.compile(r'<[^>]+>')
                    caption = htmlTagPattern.sub("",caption)		
					#字符太多就截取
                    if len(caption) > 50:
                        caption = caption[0:50]			
                    caption=post["@id"]+"_"+caption						
				    # 超过天数的就不下载
                    if ((DOWN_LIMIT_DATE - datetime.datetime.strptime(post["@date-gmt"][0:10], '%Y-%m-%d')).days) > 0 :
                        breakFlag=False
                        break
                    # select the largest resolution
                    # usually in the first element			
                    else:
                        if medium_type == "photo":	
						    #相片类型，把子对象全部加入到队列中
                            photos =post["photoset"]["photo"]
                            for photo in photos:
                                self.queue.put((medium_type, photo, target_folder,caption))
                        else:
                            self.queue.put((medium_type, post, target_folder,caption))
                start += MEDIA_NUM
            except Exception as err:
                logger.info("Error " ,err)			
                break


def usage():
    logger.info(u"未找到sites.txt文件，请创建.\n"
          u"请在文件中指定Tumblr站点名，并以逗号分割，不要有空格.\n"
          u"保存文件并重试.\n\n"
          u"例子: site1,site2\n\n"
          u"或者直接使用命令行参数指定站点\n"
          u"例子: python tumblr-photo-video-ripper.py site1,site2")


def illegal_json():
    logger.info(u"文件proxies.json格式非法.\n"
          u"请参照示例文件'proxies_sample1.json'和'proxies_sample2.json'.\n"
          u"然后去 http://jsonlint.com/ 进行验证.")


if __name__ == "__main__":
    sites = None

    proxies = None
    if os.path.exists("./proxies.json"):
        with open("./proxies.json", "r") as fj:
            try:
                proxies = json.load(fj)
                if proxies is not None and len(proxies) > 0:
                    logger.info("You are using proxies.\n%s" % proxies)
            except:
                illegal_json()
                sys.exit(1)

    if len(sys.argv) < 2:
        #校验sites配置文件
        filename = "sites.txt"
        if os.path.exists(filename):
            with open(filename, "r") as f:
                sites = f.read().rstrip().lstrip().split(",")
        else:
            usage()
            sys.exit(1)
    else:
        sites = sys.argv[1].split(",")

    if len(sites) == 0 or sites[0] == "":
        usage()
        sys.exit(1)

    CrawlerScheduler(sites, proxies=proxies)
