package com.seansoper.zebec

import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import kotlin.system.exitProcess

object Core {

    @JvmStatic fun main(args: Array<String>) {
        val cli = CommandLineParser(args)

        if (cli.shouldShowHelp) {
            cli.showHelp()
            exitProcess(0)
        }

        val (source, dest, port, extensions, verbose) = cli.parse()?.let {
            it
        } ?: run {
            println(cli.errorMessage)
            cli.showHelp()
            exitProcess(1)
        }

        val server = ContentServer(dest, port, verbose)
        server.start()

        runBlocking {
            watchFiles(source, dest, extensions, server, verbose)
        }
    }

    suspend fun watchFiles(source: Path, dest: Path, extensions: List<String>, server: ContentServer, verbose: Boolean) {
        val watch = try {
            WatchFile(listOf(source), extensions)
        } catch (exception: NoSuchFileException) {
            println("ERROR: watch argument invalid for ${exception.file}")
            exitProcess(1)
        }

        val changes = watch.watchChanges()

        if (verbose) {
            watch.paths.forEach { println("Watching $it") }
            println("Filtering on files with extensions ${extensions.joinToString()}")
        }

        changes.collect { changed ->
            if (verbose) {
                println("Change detected at ${changed.path}")
            }

            EventProcessor(changed, source, dest, verbose).process {
                if (verbose) {
                    it?.let {
                        println("Copied to $it")
                    } ?: println("Failed to compile")
                }

                server.refresh()
            }
        }
    }
}