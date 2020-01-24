package com.seansoper.zebec.FileProcessor

import com.seansoper.zebec.Utilities.humanReadableByteCount
import com.seansoper.zebec.WatchFile
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

interface Processable {
    fun process(content: String): String?
}

class EventHandler(val changed: WatchFile.ChangedFile, val source: Path, val dest: Path, val verbose: Boolean = false) {

    private data class ProcessedDirs(val dir: Path, val parentDir: File)
    private data class ProcessedFile(val content: String, val fullname: String)

    fun process(done: (Path?) -> Unit) {
        val path = processFile { filename, extension, content ->
            when (extension) {
                "ktml" -> HTML(verbose).process(content)?.let {
                    ProcessedFile(it, "$filename.html")
                }
                "js" -> Script(Script.Type.javascript, verbose).process(content)?.let {
                    ProcessedFile(it, "$filename.min.$extension")
                }
                "css" -> Script(Script.Type.stylesheet, verbose).process(content)?.let {
                    ProcessedFile(it, "$filename.min.$extension")
                }
                else -> {
                    if (verbose) {
                        println("ERROR: Unsupported content type $extension")
                    }

                    null
                }
            }
        }

        done(path)
    }

    private fun getDirectories(changedPath: Path, source: Path, dest: Path): ProcessedDirs? {
        val destDir = changedPath.toString().split(source.toString()).elementAtOrNull(1) ?: return null
        val dir = Paths.get(dest.toString(), destDir)
        val parentDir = File(dir.toString().replace("/./", "/")).parentFile

        return ProcessedDirs(dir, parentDir)
    }

    private fun processFile(transform: (filename: String, extension: String, content: String) -> ProcessedFile?): Path? {
        val (dir, parentDir) = getDirectories(changed.path, source, dest) ?: return null
        val filename = dir.fileName.toString().split(".").firstOrNull() ?: return null
        val content = File(changed.path.toString()).readText()
        val result = transform(filename, changed.extension, content) ?: return null

        if (!parentDir.exists()) {
            parentDir.mkdirs()
        }

        val path = Paths.get(parentDir.toString(), result.fullname)

        if (verbose) {
            val origSize = humanReadableByteCount(content.length)
            val newSize = humanReadableByteCount(result.content.length)
            println("Compiled ${dir.fileName}, $origSize → $newSize")
        }

        Files.write(path, result.content.toByteArray())
        return path
    }
}