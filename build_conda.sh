#!/bin/bash
CONDA_HOME=$HOME/anaconda3
# This is a script to create and build hamilton for conda.
# Be sure you have conda activated and have logged into anaconda
# conda activate && anaconda login
pkg='sf-hamilton'
# adjust the Python versions you would like to build
array=(3.7 3.8 3.9 3.10 3.11 )
echo "Building conda package ..."
cd ~
# this will create a ~/sf-hamilton directory with metadata to build the package.
# will error if it already exists.
conda skeleton pypi $pkg

# building conda packages
for i in "${array[@]}"
do
	conda build --python $i $pkg
done
# convert package to other platforms
cd ~
# platforms=( linux-64 linux-32 linux-64 win-32 win-64 )
find $CONDA_HOME/conda-bld/osx-64/ -name *.tar.bz2 | while read file
do
    echo $file
    conda convert --platform all $file  -o $CONDA_HOME/conda-bld/
    #for platform in "${platforms[@]}"
    #do
    #   conda convert --platform $platform $file  -o $CONDA_HOME/conda-bld/
    #done
done
# upload packages to conda
# run `conda install anaconda-client` to install `anaconda` and do `anaconda login` (conda activate first)
find $CONDA_HOME/conda-bld/ -name "*.tar.bz2" | while read file
do
    echo $file
    anaconda upload -u Hamilton-OpenSource $file
done
echo "Built & uploaded conda package done!"
echo "To purge the build files, run: conda build purge; and then delete *.tar.bz2 files under $CONDA_HOME/conda-bld/"
