package com.seansoper.zebec

typealias TagAttributes = Map<String, String>

class KTMLParser {

    abstract class Element(val type: String, val attributes: TagAttributes? = null) {
        abstract fun render(indent: Int = 0): String
        val tagAttributes: String = attributes?.map { " ${it.key}='${it.value}'" }?.joinToString("") ?: ""
    }

    interface SupportsATag {
        fun aTag(text: String, href: String, attributes: TagAttributes? = null, addTag: (TagAttributes) -> Unit) {
            attributes?.also {
                val finalAttrs = it.toMutableMap()
                finalAttrs["href"] = href
                addTag(finalAttrs)
            } ?: run {
                addTag(mapOf("href" to href))
            }
        }
    }

    enum class LinkRelType(val value: String) {
        Shortcut("shortcut icon"),
        Stylesheet("stylesheet"),
        Alternate("alternate")
    }

    interface SupportsLinkTag {
        fun linkTag(relType: LinkRelType, attributes: TagAttributes, addTag: (TagAttributes) -> Unit) {
            val finalAttrs = attributes.toMutableMap()
            finalAttrs["rel"] = relType.value
            addTag(finalAttrs)
        }
    }

    interface SupportsScriptTag {
        fun scriptTag(src: String, attributes: TagAttributes? = null, addTag: (TagAttributes) -> Unit) {
            attributes?.also {
                val finalAttrs = it.toMutableMap()
                finalAttrs["src"] = src
                addTag(finalAttrs)
            } ?: run {
                addTag(mapOf("src" to src))
            }
        }
    }

    // TODO: Coalesce image, script and other similar tags
    interface SupportsImageTag {
        fun imageTag(src: String, attributes: TagAttributes? = null, addTag: (TagAttributes) -> Unit) {
            attributes?.also {
                val finalAttrs = it.toMutableMap()
                finalAttrs["src"] = src

                if (!finalAttrs.containsKey("alt")) {
                    finalAttrs["alt"] = ""
                }

                addTag(finalAttrs)
            } ?: run {
                addTag(mapOf("src" to src, "alt" to ""))
            }
        }
    }

    interface SupportsInputTag {
        fun inputTag(type: String, attributes: TagAttributes? = null, addTag: (TagAttributes) -> Unit) {
            attributes?.also {
                val finalAttrs = it.toMutableMap()
                finalAttrs["type"] = type
                addTag(finalAttrs)
            } ?: run {
                addTag(mapOf("type" to type))
            }
        }
    }

    abstract class Tag(type: String, attributes: TagAttributes? = null) : Element(type, attributes) {
        var children: Array<Element> = emptyArray()

        override fun render(indent: Int): String {
            val indentation = " ".repeat(indent)
            var str = "$indentation<$type$tagAttributes>\n"
            str += children.joinToString("\n") { it.render(indent + 2) }
            str += "\n$indentation</$type>"

            return str
        }

        fun <T : Tag> initTag(tag: T, init: T.() -> Unit): T {
            tag.init()
            children += tag
            return tag
        }

        fun addTag(tag: Element) {
            children += tag
        }
    }

    class TagWithText(type: String, val text: String, attributes: TagAttributes? = null) : Element(type, attributes) {
        override fun render(indent: Int): String {
            val indentation = " ".repeat(indent)
            return "$indentation<$type$tagAttributes>$text</$type>"
        }
    }

    class TagSelfClosing(type: String, attributes: TagAttributes?) : Element(type, attributes) {
        override fun render(indent: Int): String {
            val indentation = " ".repeat(indent)
            return "$indentation<$type$tagAttributes />"
        }
    }

    class TagComment(val comment: String) : Element("comment") {
        override fun render(indent: Int): String {
            val indentation = " ".repeat(indent)
            return "$indentation<!-- $comment -->"
        }
    }

    class TagConditionalComment(val condition: String) : Tag("comment"), SupportsScriptTag {
        override fun render(indent: Int): String {
            val indentation = " ".repeat(indent)
            var str = "$indentation<!--[if $condition]>\n"
            str += children.joinToString("\n") { it.render(indent + 2) }
            str += "\n$indentation<![endif]-->"

            return str
        }

        fun script(src: String) {
            scriptTag(src) {
                addTag(TagWithText("script", "", it))
            }
        }
    }

    class HTML(language: String) : Tag("html", mapOf("lang" to language)) {

        override fun render(indent: Int): String {
            val content = super.render(indent)
            return "<!doctype html>\n$content"
        }

        fun head(init: Head.() -> Unit) = initTag(Head(), init)
        fun body(init: Body.() -> Unit) = initTag(Body(), init)
    }

    class Head : Tag("head"), SupportsLinkTag, SupportsScriptTag {
        fun title(text: String) = addTag(TagWithText("title", text))
        fun meta(attributes: TagAttributes) = addTag(TagSelfClosing("meta", attributes))
        fun comment(comment: String) = addTag(TagComment(comment))
        fun ifComment(condition: String, init: TagConditionalComment.() -> Unit) =
            initTag(TagConditionalComment(condition), init)

        fun link(relType: LinkRelType, attributes: TagAttributes) {
            linkTag(relType, attributes) {
                addTag(TagSelfClosing("link", it))
            }
        }

        fun script(src: String) {
            scriptTag(src) {
                addTag(TagWithText("script", "", it))
            }
        }
    }

    class Body : Tag("body"), SupportsScriptTag {
        fun div(attributes: TagAttributes?, init: DivTag.() -> Unit) = initTag(DivTag(attributes), init)
        fun nav(attributes: TagAttributes?, init: NavTag.() -> Unit) = initTag(NavTag(attributes), init)
        fun comment(comment: String) = addTag(TagComment(comment))
        fun noscript(attributes: TagAttributes?, init: NoScriptTag.() -> Unit) = initTag(NoScriptTag(attributes), init)
        fun gaTag(site: String) = addTag(GoogleAnalyticsTag(site))
        fun script(src: String, attributes: TagAttributes? = null) {
            scriptTag(src, attributes) {
                addTag(TagWithText("script", "", it))
            }
        }
    }

    class NavTag(attributes: TagAttributes?) : Tag("nav", attributes) {
        fun div(attributes: TagAttributes?, init: DivTag.() -> Unit) = initTag(DivTag(attributes), init)
    }

    class NoScriptTag(attributes: TagAttributes?) : Tag("noscript", attributes), SupportsLinkTag {
        fun link(relType: LinkRelType, attributes: TagAttributes) {
            linkTag(relType, attributes) {
                addTag(TagSelfClosing("link", it))
            }
        }
    }

    class DivTag(attributes: TagAttributes?) : Tag("div", attributes), SupportsImageTag, SupportsInputTag, SupportsATag {
        fun div(attributes: TagAttributes?, init: DivTag.() -> Unit) = initTag(DivTag(attributes), init)
        fun p(attributes: TagAttributes?, init: PTag.() -> Unit) = initTag(PTag(attributes), init)
        fun p(text: String, attributes: TagAttributes? = null) = addTag(TagWithText("p", text, attributes))
        fun h1(text: String, attributes: TagAttributes? = null) = addTag(TagWithText("h1", text, attributes))
        fun h2(text: String, attributes: TagAttributes? = null) = addTag(TagWithText("h2", text, attributes))
        fun h3(text: String, attributes: TagAttributes? = null) = addTag(TagWithText("h3", text, attributes))
        fun h4(text: String, attributes: TagAttributes? = null) = addTag(TagWithText("h4", text, attributes))
        fun h5(text: String, attributes: TagAttributes? = null) = addTag(TagWithText("h5", text, attributes))
        fun blockquote(text: String, attributes: TagAttributes? = null) = addTag(TagWithText("blockquote", text, attributes))
        fun ul(attributes: TagAttributes? = null, init: UlTag.() -> Unit) = initTag(UlTag(attributes), init)
        fun button(attributes: TagAttributes? = null, init: ButtonTag.() -> Unit) = initTag(ButtonTag(attributes), init)
        fun hr(attributes: TagAttributes? = null) = addTag(TagSelfClosing("hr", attributes))
        fun span(attributes: TagAttributes? = null, init: SpanTag.() -> Unit) = initTag(SpanTag(attributes), init)
        fun comment(comment: String) = addTag(TagComment(comment))
        fun raw(content: String) = addTag(Raw(content))
        fun content() = addTag(TagSelfClosing("zebeccontent", null))

        fun image(src: String, attributes: TagAttributes? = null) {
            imageTag(src, attributes) {
                addTag(TagSelfClosing("img", it))
            }
        }

        fun input(type: String, attributes: TagAttributes?) {
            inputTag(type, attributes) {
                addTag(TagSelfClosing("input", it))
            }
        }

        fun a(text: String, href: String, attributes: TagAttributes? = null) {
            aTag(text, href, attributes) {
                addTag(TagWithText("a", text, it))
            }
        }
    }

    class Raw(val content: String) : Element("raw") {
        override fun render(indent: Int): String {
            val indentation = " ".repeat(indent)
            return "$indentation$content"
        }
    }

    class UlTag(attributes: TagAttributes?) : Tag("ul", attributes) {
        fun li(attributes: TagAttributes? = null, init: LiTag.() -> Unit) = initTag(LiTag(attributes), init)
        fun li(text: String, attributes: TagAttributes? = null) = addTag(TagWithText("li", text, attributes))
    }

    class LiTag(attributes: TagAttributes?) : Tag("li", attributes), SupportsATag {
        fun a(text: String, href: String, attributes: TagAttributes? = null) {
            aTag(text, href, attributes) {
                addTag(TagWithText("a", text, it))
            }
        }
    }

    class ButtonTag(attributes: TagAttributes?) : Tag("button", attributes) {
        fun span(text: String, attributes: TagAttributes?) = addTag(TagWithText("span", text, attributes))
    }

    class PTag(attributes: TagAttributes?) : Tag("p", attributes), SupportsATag {
        fun a(text: String, href: String, attributes: TagAttributes? = null) {
            aTag(text, href, attributes) {
                addTag(TagWithText("a", text, it))
            }
        }
    }

    class SpanTag(attributes: TagAttributes?): Tag("span", attributes) {
        fun button(text: String, attributes: TagAttributes?) = addTag(TagWithText("button", text, attributes))
    }

    class GoogleAnalyticsTag(val site: String) : Element("script") {
        override fun render(indent: Int): String {
            val indentation = " ".repeat(indent)
            return "${indentation}<script async src='https://www.googletagmanager.com/gtag/js?id=${site}'></script>\n" +
                   "${indentation}<script>\n" +
                   "${indentation}window.dataLayer = window.dataLayer || [];\n" +
                   "${indentation}function gtag(){dataLayer.push(arguments);}\n" +
                   "${indentation}gtag('js', new Date());\n" +
                   "${indentation}gtag('config', '${site}');\n" +
                   "${indentation}</script>"
        }
    }

    fun html(language: String = "en", init: HTML.() -> Unit): HTML {
        val result = HTML(language)
        result.init()
        return result
    }
}
