## git笔记

## git常用命令

```shell
#设置用户签名
git config --global user.name "Give-me-meat"
git config --global user.email "15852907568@163.com"
#初始化本地库
git init
#查看git状态
git status
#添加文件至暂存区
git add hello.txt
#删除暂存区文件
git rm --cached hello.txt
#首次提交
git commit -m "first commit" hello.txt
#版本信息
git reflog
git log
#切换版本
git reset --hard e78eed5
#查看分支
git branch -v
#创建分支
git branch lin-test 
#切换分支
git checkout lin-test
#合并分支
git merge lin-test 
#创建远程库以及创建别名
git remote add git-demo https://github.com/Give-me-meat/git-demo.git
#push
git push git-demo master
#pull
git pull git-demo master
#clone
git clone git-demo master
#查看远程库地址
git remote -v
#生成秘钥
ssh-keygen -t rsa -C "15852907568@163.com"
ssh-agent -s
```



## git.ignore文件

```shell
# Compiled class file
*.class

# Log file
*.log

# BlueJ files
*.ctxt

# Mobile Tools for Java (J2ME)
.mtj.tmp/

# Package Files #
*.jar
*.war
*.nar
*.ear
*.zip
*.tar.gz
*.rar


# virtual machine crash logs,see http://www.java.com/en/download/help/error_hotspot.xml hs_err_pid*

.classpath
.project
.settings target
.idea
*.iml

```



## .gitconfig

```shell
[user]
	name = Give-me-meat
	email = 15852907568@163.com

[core]
	excludesfile = C:/Users/Admin/git.ignore
```





## git工作机制

工作区——>暂存区——>本地库



## git版本切换原理

head指针指向不同版本



## clone操作

- 拉取代码
- 初始化本地仓库
- 创建别名（默认为origin）