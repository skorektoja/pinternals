@echo off
rem jar.exe -cfvm diffo.jar MANIFEST.MF logging.properties com/pinternals/diffo/* saplib/* lib/* 
jar.exe -cfvm diffo.jar MANIFEST.MF logging.properties com/pinternals/diffo/*
jar.exe -cfv diffo_api.jar com/pinternals/diffo/api/*
@echo done
