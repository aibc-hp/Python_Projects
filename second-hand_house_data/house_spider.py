from requests_html import HTMLSession  # 导入HTMLSession类
from requests_html import HTML  # 导入HTML类
from multiprocessing import Pool  # 导入进程池
import matplotlib  # 导入图表模块
import matplotlib.pyplot as plt  # 导入绘图模块
import pandas as pd  # 导入pandas模块

# 避免中文乱码
matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 用了正常显示中文标签
matplotlib.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

# 分类列表，作为数据表中的列名
class_name_list = ['小区名字', '总价', '户型', '建筑面积', '单价', '区域']
# 创建DataFrame临时表格
df = pd.DataFrame(columns=class_name_list)

# 创建控制台菜单menu()方法，在该方法中使用print()函数打印一个菜单选项
def menu():
    print('''
    ┌--------------南昌市二手房数据查询系统--------------┐
    |                                               |
    |=================== 功能菜单 ====================|
    |                                               |
    |            1 爬取南昌市最新二手房数据              |
    |            2 查看南昌市二手房总数量               |
    |            3 查看南昌市二手房均价                 |         
    |            4 查看南昌市各区二手房数量比例           |
    |            5 查看南昌市各区二手房均价              |
    |            6 查看南昌市热门户型均价               |
    |            0 退出系统                           |
    |                                               |
    |===============================================|
    └-----------------------------------------------┘
    ''')

# 创建main()方法，在该方法中根据用户在控制台中所输入的选项来启动对应功能
def main():
    ctrl = True  # 标记是否退出系统
    while ctrl:
        menu()  # 显示菜单
        option = eval(input('请选择: '))  # 选择菜单项
        if option == 0:  # 退出系统
            print('退出南昌市二手房数据查询系统！')
            ctrl = False
        elif option == 1:
            print('爬取南昌市最新二手房数据')
            start_crawler()  # 启动多进程爬虫
        elif option == 2:
            print('查看南昌市二手房总数量')
            show_house_number()  # 显示二手房总数量
        elif option == 3:
            print('查看南昌市二手房均价')
            show_average_price()  # 显示二手房均价
        elif option == 4:
            print('查看南昌市各区二手房数量比例')
            show_area_house_number()  # 显示各区二手房数量比例
        elif option == 5:
            print('查看南昌市各区二手房均价')
            show_area_average_price()  # 显示各区二手房均价
        elif option == 6:
            print('查看南昌市热门户型均价')
            show_popular_house_type()  # 显示热门户型均价
        else:
            print('请输入正确的功能选项！')

# 创建get_ua()方法，每次调用get_ua()方法时都会返回请求头ua信息
def get_ua():
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36 Edg/103.0.1264.49'
    return ua

# 创建get_house_info()方法，在该方法中先创建一个HTMLSession会话对象，然后发送网络请求获取每个房子的详情页地址，再向详情页地址发送二次请求，最后从详情页响应内容中提取并保存二手房的相关数据
def get_house_info(url):
    session = HTMLSession()  # 创建HTML会话对象
    ua = get_ua()  # 获取请求头信息
    response = session.get(url, headers={'User-Agent': ua})  # 发送网络请求
    if response.status_code == 200:  # 判断请求是否成功
        html = HTML(html=response.text)  # 解析HTML
        hrefs = html.xpath('//div[@class="title"]/a/@href')  # 获取详情页地址
        for href in hrefs:
            try:
                ua = get_ua()  # 获取请求头信息
                response_info = session.get(url=href, headers={'User-Agent': ua})  # 发送获取详细信息的网络请求
                html_info = HTML(html=response_info.text)  # 解析HTML
                name = html_info.xpath('//div[@class="communityName"]/a[1]/text()')[0]  # 获取小区名字
                total_price = html_info.xpath('//span[@class="total"]/text()')[0]  # 获取房子总价
                region = html_info.xpath('//span[@class="info"]/a[1]/text()')[0]  # 获取房子所在区域
                unit_price = html_info.xpath('//span[@class="unitPriceValue"]/text()')[0]  # 获取房子单价
                house_type = html_info.xpath('//div[@class="room"]/div[1]/text()')[0]  # 获取房子户型
                floor_area = html_info.xpath('//div[@class="area"]/div[1]/text()')[0]  # 获取房子建筑面积
                # 小区名字，总价，户型，建筑面积，单价，区域
                print(name, total_price, house_type, floor_area, unit_price, region)
                # 将数据信息添加到DataFrame临时表格中
                df.loc[len(df)+1] = {'小区名字': name, '总价': total_price, '户型': house_type, '建筑面积': floor_area, '单价': unit_price, '区域': region}
            except:
                pass  # 遇到异常跳过
        # 将数据以添加模式写入csv文件中，不再添加头部列
        df.to_csv('南昌市二手房数据.csv', mode='a', header=False)
    else:
        print(response.status_code)

# 创建start_crawler()方法，在该方法中创建4进程对象，而后通过进程对象启动爬虫
def start_crawler():
    df.to_csv('南昌市二手房数据.csv', encoding='utf_8_sig')  # 第一次生成带表头的空文件
    url = 'https://nc.lianjia.com/ershoufang/pg{}/'
    urls = [url.format(str(i)) for i in range(1, 100)]  # 将每一页的url作为元素存放在urls列表中
    pool = Pool(processes=4)  # 创建4进程对象
    pool.map(get_house_info, urls)
    pool.close()  # 关闭进程池

# 创建cleaning_data()方法，在该方法中首先读取刚刚爬取的南昌市二手房数据.csv文件，并创建DataFrame临时表格，然后将数据中的索引列、空值以及无效值删除，最后返回清洗后的数据
def cleaning_data():
    data = pd.read_csv('南昌市二手房数据.csv')  # 读取csv文件
    del data['Unnamed: 0']  # 将索引列删除
    data.dropna(axis=0, how='any', inplace=True)  # 删除data数据中的所有空值
    data = data.drop_duplicates()  # 删除重复数据
    return data

# 创建show_house_number()方法，在该方法中首先获取已经清洗后的二手房数据，然后统计房子总数量
def show_house_number():
    data = cleaning_data()  # 获取清洗后的数据
    print(data.shape[0])

# 创建show_average_price()方法，在该方法中首先获取已经清洗后的二手房数据，然后统计房子均价
def show_average_price():
    data = cleaning_data()  # 获取清洗后的数据
    all_price = data['总价'].sum(axis=0)
    print(all_price // data.shape[0])

# 创建show_area_house_number()方法，在该方法中首先获取已经清洗后的二手房数据，然后根据区域进行分组并获取每个区域房子的数量，再计算出各个区域二手房数量的百分比，最后通过饼图可视化出来
def show_area_house_number():
    data = cleaning_data()  # 获取清洗后的数据
    group_number = data.groupby('区域').size()  # 房子区域分组数量
    region = group_number.index  # 区域
    numbers = group_number.values  # 获取各区域内房子的数量
    percentage = numbers / numbers.sum() * 100  # 计算各区域房子数量的百分比
    # 可视化
    plt.figure()
    plt.pie(percentage, labels=region, labeldistance=1.05, autopct='%1.1f%%', shadow=True, startangle=0, pctdistance=0.6)  # 绘制饼图
    plt.axis('equal')  # 设置横轴和纵轴大小相等，这样饼才是圆形
    plt.title('南昌市各区二手房数量百分比', fontsize=12)
    plt.show()  # 显示饼图

# 创建show_area_average_price()方法，在该方法中首先获取已经清洗后的二手房数据，然后根据区域进行分组并计算出各区域的均价，最后通过垂直柱形图可视化出来
def show_area_average_price():
    data = cleaning_data()  # 获取清洗后的数据
    group = data.groupby('区域')  # 房子区域分组
    average_price_group = group['单价'].mean()  # 计算各区域二手房的均价
    region = average_price_group.index  # 区域
    average_price = average_price_group.values.astype(int)  # 区域对应的均价
    # 可视化
    plt.figure()
    plt.bar(region, average_price, alpha=0.8)  # 绘制柱形图
    plt.xlabel('区域')  # 横轴标签
    plt.ylabel('均价')  # 纵轴标签
    plt.title('南昌市各区二手房均价')
    for x, y in enumerate(average_price):  # 为每一个图形加数值标签
        plt.text(x, y+100, y, ha='center')
    plt.show()  # 显示柱形图

# 创建show_popular_house_type()方法，在该方法中首先获取已经清洗后的二手房数据，然后根据户型进行分组并统计每个分组的数量，再根据户型分组的数量进行降序并提取前五组户型数据，而后计算每个户型的均价，最后通过水平柱形图可视化出来
def show_popular_house_type():
    data = cleaning_data()  # 获取清洗后的数据
    house_type_number = data.groupby('户型').size()  # 房子户型分组数量
    sort_values = house_type_number.sort_values(ascending=False)  # 将户型分组数量进行降序
    top_five = sort_values.head(5)  # 提取5组户型数据
    house_type_mean = data.groupby('户型')['单价'].mean()  # 计算每个户型的均价
    type = house_type_mean[top_five.index].index  # 户型
    price = house_type_mean[top_five.index].values  # 户型对应的均价
    price = price.astype(int)
    # 可视化
    plt.figure()
    # 从下往上画水平柱形图
    plt.barh(type, price, height=0.3, color='r', alpha=0.8)
    plt.xlim(0, 15000)
    plt.xlabel('均价')
    plt.title('热门户型均价')
    for y, x in enumerate(price):  # 为每一个图形加数值标签
        plt.text(x+10, y, str(x)+'元', va='center')
    plt.show()  # 显示图形

# 创建程序入口，并调用自定义的main()方法
if __name__ == '__main__':
    main()

