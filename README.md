# USTC-RA
## Describe:保存一些在USTC做RA时写的代码，数据由于保密协议不进行上传

## lawDataProcess.py（2022.09.09）
# 难点
- 数据量太大，内存较小，不能直接从MySQL数据库拉取
- 需要使用MySQL语言进行交互
# 解决key
- 拉取数据时占用内存较大，但储存在Python里的数据不会占用太大的运行内存，因此先拉取一部分字符少的信息，获取数据的ID，根据ID进行匹配，分批拉取数据
# 解决方案
- 分批拉取数据
- 将Text这个大数据类型储存在Txt中，Excel附Txt链接（未尝试，但可能会占用大量的磁盘空间
