@echo off
@title bat����ִ��git������ʾ

set path=%path%;
git add -v .
git commit -m "post"
git push -u origin master

echo\&echo done...
pause