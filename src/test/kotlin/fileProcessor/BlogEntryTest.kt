package fileProcessor

import com.seansoper.zebec.fileProcessor.BlogEntry
import com.seansoper.zebec.fileProcessor.Markdown
import io.kotlintest.matchers.boolean.shouldBeTrue
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

class BlogEntryTest: StringSpec({

    "metadata not in html" {
        val source = """
            [//]: # (ztitle: This is the title)
            This is the **content**
        """.trimIndent()
        val expected = "<body><p>This is the <strong>content</strong></p></body>"
        val result = BlogEntry().process(source)!!
        result.shouldBe(expected)
    }

    "parse metadata" {
        val author = "Sinead O’Connor St. Mark's"
        val title = "This is. A, title. it’s your's. Also 9"
        val subtitle = "This is the subtitle"
        val image = "http://placekitten.com/900/300"
        val tags = "kotlin, programming,java"
        val template = "blog"
        val source = """
            [//]: # (zauthor: $author)
            [//]: # (ztitle: $title)
            [//]: # (zsubtitle: $subtitle)
            [//]: # (zimage: $image)
            [//]: # (ztags: $tags)
            [//]: # (ztemplate: $template)
            This is the **content**
        """.trimIndent()

        val blog = BlogEntry().parseMetadata(source)!!
        blog.author.shouldBe(author)
        blog.title.shouldBe(title)
        blog.imageURL!!.toString().shouldBe(image)
        blog.subtitle!!.shouldBe(subtitle)
        blog.template!!.shouldBe(template)

        val strTags = tags.split(", ?".toRegex())
        strTags.count().shouldBe(3)
        strTags.forEach { blog.tags.contains(it).shouldBeTrue() }
    }
})