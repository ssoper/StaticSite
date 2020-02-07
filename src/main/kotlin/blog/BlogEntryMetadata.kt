package com.seansoper.zebec.blog

import com.seansoper.zebec.fileProcessor.Markdown
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*

class InvalidAuthorException: Exception("No author found")
class InvalidTitleException: Exception("No title found")

class BlogEntryMetadata(val path: Path) {
    val author: String
    val title: String
    val tags: Array<String>
    val image: BlogImage?
    val subtitle: String?
    val firstParagraph: String?

    val createdDate: LocalDateTime
        get() {
            return try {
                (Files.getAttribute(path, "creationTime") as FileTime).
                    toInstant().
                    atOffset(ZoneOffset.UTC).
                    toLocalDateTime()
            } catch (exception: IOException) {
                Instant.now().atOffset(ZoneOffset.UTC).toLocalDateTime()
            }
        }

    val entryHtml: String
        get() {
            var result = "<h1 class='mt-4'>$title</h1>"
            subtitle?.let { result += "<h2 class='subtitle'>$subtitle</h2>" }

            result += """
                    <div class='author'>
                      <img src='/images/avatar.jpg' alt='' />
                      <ul>
                        <li>$author</li>
                        <li>$createdDate</li>
                      </ul>
                    </div>
                """.trimIndent()

            image?.let {
                result += "<img class='img-fluid rounded' alt='' ${it.imageHtmlAttributes(BlogImage.Type.Entry)}>"
            }

            return result
        }

    private val Formatter = DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.US)
    private val TitleRegex = "([a-z0-9]+(([’',. -][a-z0-9 ])?[a-z0-9]*)*)"

    init {
        val content = File(path.toString()).readText()

        author = parseAuthor(content) ?: throw InvalidAuthorException()
        title = parseTitle(content) ?: throw InvalidTitleException()
        tags = parseTags(content)
        image = parseImageURL(content)?.let { BlogImage(it) }
        subtitle = parseSubtitle(content)
        firstParagraph = Markdown().getFirstParagraph(content)
    }

    fun previewHtml(relativePath: String): String {
        val image = image?.let {
            "<img class='card-img-top' alt='' ${it.imageHtmlAttributes(BlogImage.Type.Preview)} />"
        } ?: ""

        return """
                <div class='card mb-4'>
                    $image
                    <div class='card-body p-3'>
                      <h5 class='card-title m-0'>${title}</h5>
                      <p class='card-text'>${firstParagraph}</p>
                      <a href='${relativePath}' class='stretched-link'></a>
                    </div>
                  </div>
            """.trimIndent()
    }

    private fun parseAuthor(content: String): String? {
        val regex = Regex("^\\[//]: # \\(zauthor: ([a-z]+(([’',. -][a-z ])?[a-z]*)*)\\)$", setOf(RegexOption.MULTILINE, RegexOption.IGNORE_CASE))
        return regex.find(content)?.groups?.get(1)?.value
    }

    private fun parseTitle(content: String): String? {
        val regex = Regex("^\\[//]: # \\(ztitle: $TitleRegex\\)$", setOf(RegexOption.MULTILINE, RegexOption.IGNORE_CASE))
        return regex.find(content)?.groups?.get(1)?.value
    }

    private fun parseSubtitle(content: String): String? {
        val regex = Regex("^\\[//]: # \\(zsubtitle: $TitleRegex\\)$", setOf(RegexOption.MULTILINE, RegexOption.IGNORE_CASE))
        return regex.find(content)?.groups?.get(1)?.value
    }

    private fun parseImageURL(content: String): URL? {
        val regex = Regex("^\\[//]: # \\(zimage: (https?://[^\\s/\$.?#].[^\\s]*)\\)$", setOf(RegexOption.MULTILINE, RegexOption.IGNORE_CASE))
        return regex.find(content)?.groups?.get(1)?.value.let { URL(it) }
    }

    private fun parseTags(content: String): Array<String> {
        val regex = Regex("^\\[//]: # \\(ztags: ([a-z]+(, ?[a-z]+)*)\\)$", setOf(RegexOption.MULTILINE, RegexOption.IGNORE_CASE))
        return regex.find(content)?.groups?.get(1)?.value?.split(", ?".toRegex())?.toTypedArray() ?: emptyArray()
    }
}