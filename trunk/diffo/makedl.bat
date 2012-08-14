@echo off
set V=0.1.0
7z a diffo_%V%.zip diffo.bat diffo.jar lib/*.jar saplib/*.jar diffo_sample.bat
