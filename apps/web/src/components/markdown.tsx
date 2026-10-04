import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import "./markdown.css";

const remarkPlugins = [remarkGfm];

export function Markdown({ children }: { children: string }) {
  return (
    <div className="markdown-content">
      <ReactMarkdown remarkPlugins={remarkPlugins} skipHtml>
        {children}
      </ReactMarkdown>
    </div>
  );
}
