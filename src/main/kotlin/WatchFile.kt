package com.seansoper.zebec

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY
import java.nio.file.WatchService

class WatchFile(paths: List<Path>, val extensions: List<String>) {
    val watchService: WatchService = FileSystems.getDefault().newWatchService()
    val paths: Set<Path>

    init {
        this.paths = getSubdirectories(paths)
        this.paths.forEach {
            it.register(watchService, ENTRY_MODIFY)
        }
    }

    data class ChangedFile(val path: Path, val extension: String)

    suspend fun watchChanges(): Flow<ChangedFile> {
        return flow {
            withContext(Dispatchers.IO) {
                while (true) {
                    val key = watchService.take()
                    val dirPath = key.watchable() as? Path ?: break
                    key.pollEvents().forEach {
                        val path = dirPath.resolve(it.context() as Path)
                        matches(path)?.let { extension ->
                            emit(ChangedFile(path, extension))
                        }
                    }
                    key.reset()
                }
            }
        }
    }

    private fun matches(path: Path): String? {
        return extensions.firstOrNull { path.toString().toLowerCase().endsWith(".${it.toLowerCase()}") }
    }

    private fun getSubdirectories(paths: List<Path>): Set<Path> {
        val results = mutableSetOf<Path>()

        paths.forEach {
            File(it.toString()).walk().filter { it.isDirectory }.forEach {
                results.add(it.toPath())
            }
        }

        return results
    }
}