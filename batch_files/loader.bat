@echo off
REM this batch file is intended to be run from the terminal with one argument
REM arg1: the name of a text file where each line is a fill link to a webpage
REM this is intended to open National Map Download Client FTP links for download

for /F "tokens=*" %%A in  (%1) do  (
   ECHO Opening %%A.... 
   start chrome %%A
)
ECHO Finished
PAUSE
@echo on
