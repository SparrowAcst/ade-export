const fs = require("fs")
const fse = require("fs-extra")
const path = require("path")
const glob = require("fast-glob")

const exists = fse.pathExists
const mkdir = dir => fse.ensureDir(path.resolve(dir))

const makeDirIfNotExists = async dir => {
    dir = path.resolve(dir)
    let dirExists = await exists(dir)
    if(!dirExists){
        await mkdir(dir)
    }
}

const rmdir = require('lignator').remove
const unlink = fs.promises.unlink

const fileList = async ( pattern, options ) => {
    pattern = pattern || "./"
    let result = await glob(pattern, options)
    return result.map( f => path.resolve(f))
}     


const dirList = async (pattern, options) => {
    
    options = extend(
        {},
        {
            includeParentPath: false,
            absolitePath: false
        },

        options
    )

    pattern = ( /\/$/.test(pattern)) ? pattern : `${pattern}/`

    let filesAndDirectories = await fse.readdir(pattern);

    let directories = [];
    await Promise.all(
        filesAndDirectories.map(name =>{
            return fse.stat(pattern + name)
            .then(stat =>{
                if(stat.isDirectory()) directories.push(name)
            })
        })
    );

    if(options.includeParentPath){
        directories = directories.map( d => pattern+d)
        if(options.absolutePath){
            directories = directories.map( d => path.resolve(d))
        }
    }
    return directories;
}


module.exports = {
    exists,
    mkdir,
    rmdir,
    unlink,
    fileList,
    dirList,
    unlink,
    makeDirIfNotExists
}