import requests
from lxml import html                 # 导入lxml.html模块

def crawl_second(url):
    #print(url)
    r = requests.get(url).content
    r_tree = html.fromstring(r)
    for i in r_tree.xpath('//a'):
        link = i.xpath('@href')[0]
        name = i.text;
        if(name is None):
            continue
        if('视频教程' not in name):
            print(name,link)


if __name__ == '__main__':
    url = 'http://zy.libraries.top/t/173.html?ma=666'
    r = requests.get(url).content
    r_tree = html.fromstring(r)
    for i in r_tree.xpath('//div/h3/a'):  # 用xpath选取节点
        link = i.xpath('@href')[0]
        name = i.xpath('span')[0].text
        #print(name, link)
        crawl_second(link)
