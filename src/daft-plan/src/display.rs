pub mod mermaid;
pub mod tree;

use std::{
    fmt::{self, Display, Write},
    sync::Arc,
};

use mermaid::{MermaidDisplayOptions, SubgraphOptions};
#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, FromPyObject, PyAny, PyResult};

pub enum DisplayFormat {
    Ascii { simple: bool },
    Mermaid(MermaidDisplayOptions),
}

#[cfg(feature = "python")]
pub struct PyDisplayFormat(pub DisplayFormat);

#[cfg(feature = "python")]
impl FromPyObject<'_> for PyDisplayFormat {
    fn extract(ob: &PyAny) -> PyResult<Self> {
        let format = ob.get_item("format")?.extract()?;
        let simple = ob.get_item("simple")?.extract()?;

        let format = match format {
            "ascii" => DisplayFormat::Ascii { simple },
            "mermaid" => {
                let subgraph_id: Option<String> = ob
                    .get_item("subgraph_id")
                    .and_then(|i| i.extract::<String>())
                    .ok();

                let name: Option<String> =
                    ob.get_item("name").and_then(|i| i.extract::<String>()).ok();

                let subgraph_options = match (name, subgraph_id) {
                    (Some(name), Some(subgraph_id)) => Some(SubgraphOptions { name, subgraph_id }),
                    (None, Some(_)) | (Some(_), None) => {
                        return Err(PyValueError::new_err(
                            "Both 'name' and 'prefix' must be provided for subgraph options",
                        ))
                    }
                    _ => None,
                };

                DisplayFormat::Mermaid(MermaidDisplayOptions {
                    simple,
                    subgraph_options,
                })
            }
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported display format: {}",
                    format
                )))
            }
        };
        Ok(PyDisplayFormat(format))
    }
}

pub(crate) trait TreeDisplay {
    // Required method: Get a list of lines representing this node. No trailing newlines.
    fn get_multiline_representation(&self) -> Vec<String>;

    // Required method: Get the human-readable name of this node.
    fn get_name(&self) -> String;

    // Required method: Get the children of the self node.
    fn get_children(&self) -> Vec<Arc<Self>>;

    // Print the whole tree represented by this node.
    fn fmt_tree(&self, s: &mut String, simple: bool) -> fmt::Result {
        self.fmt_tree_gitstyle(0, s, simple)
    }

    // Print the tree recursively, and illustrate the tree structure with a single line per node + indentation.
    fn fmt_tree_indent_style(&self, indent: usize, s: &mut String) -> fmt::Result {
        // Print the current node.
        if indent > 0 {
            writeln!(s)?;
            write!(s, "{:indent$}", "", indent = 2 * indent)?;
        }
        let node_str = self.get_multiline_representation().join(", ");
        write!(s, "{node_str}")?;

        // Recursively handle children.
        let children = self.get_children();
        match &children[..] {
            // No children - stop printing.
            [] => Ok(()),
            // One child.
            [child] => {
                // Child tree.
                child.fmt_tree_indent_style(indent + 1, s)
            }
            // Two children.
            [left, right] => {
                left.fmt_tree_indent_style(indent + 1, s)?;
                right.fmt_tree_indent_style(indent + 1, s)
            }
            _ => unreachable!("Max two child nodes expected, got {}", children.len()),
        }
    }

    // Print the tree recursively, and illustrate the tree structure in the same style as `git log --graph`.
    // `depth` is the number of forks in this node's ancestors.
    fn fmt_tree_gitstyle(&self, depth: usize, s: &mut String, simple: bool) -> fmt::Result {
        // Print the current node.
        // e.g. | | * <node contents line 1>
        //      | | | <node contents line 2>
        let lines = if simple {
            vec![self.get_name()]
        } else {
            self.get_multiline_representation()
        };
        use terminal_size::{terminal_size, Width};
        let size = terminal_size();
        let term_width = if let Some((Width(w), _)) = size {
            w as usize
        } else {
            88usize
        };

        let mut counter = 0;
        for val in lines.iter() {
            let base_characters = depth * 2;
            let expected_chars = (term_width - base_characters - 8).max(8);
            let sublines = textwrap::wrap(val, expected_chars);

            for (i, sb) in sublines.iter().enumerate() {
                self.fmt_depth(depth, s)?;
                match counter {
                    0 => write!(s, "* ")?,
                    _ => write!(s, "|   ")?,
                }
                counter += 1;
                match i {
                    0 => writeln!(s, "{sb}")?,
                    _ => writeln!(s, "  {sb}")?,
                }
            }
        }

        // Recursively handle children.
        let children = self.get_children();
        match &children[..] {
            // No children - stop printing.
            [] => Ok(()),
            // One child - print leg, then print the child tree.
            [child] => {
                // Leg: e.g. | | |
                self.fmt_depth(depth, s)?;
                writeln!(s, "|")?;

                // Child tree.
                child.fmt_tree_gitstyle(depth, s, simple)
            }
            // Two children - print legs, print right child indented, print left child.
            [left, right] => {
                // Legs: e.g. | | |\
                self.fmt_depth(depth, s)?;
                writeln!(s, "|\\")?;

                // Right child tree, indented.
                right.fmt_tree_gitstyle(depth + 1, s, simple)?;

                // Legs, e.g. | | |
                self.fmt_depth(depth, s)?;
                writeln!(s, "|")?;

                // Left child tree.
                left.fmt_tree_gitstyle(depth, s, simple)
            }
            _ => unreachable!("Max two child nodes expected, got {}", children.len()),
        }
    }

    fn fmt_depth(&self, depth: usize, s: &mut String) -> fmt::Result {
        // Print leading pipes for forks in ancestors that will be printed later.
        // e.g. "| | "
        for _ in 0..depth {
            write!(s, "| ")?;
        }
        Ok(())
    }
}

impl TreeDisplay for crate::LogicalPlan {
    fn get_multiline_representation(&self) -> Vec<String> {
        self.multiline_display()
    }

    fn get_name(&self) -> String {
        self.name()
    }

    fn get_children(&self) -> Vec<Arc<Self>> {
        self.children()
    }
}

impl TreeDisplay for crate::physical_plan::PhysicalPlan {
    fn get_multiline_representation(&self) -> Vec<String> {
        self.multiline_display()
    }

    fn get_name(&self) -> String {
        self.name()
    }

    fn get_children(&self) -> Vec<Arc<Self>> {
        self.children()
    }
}

// Single node display.
impl Display for crate::LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.get_multiline_representation().join(", "))?;
        Ok(())
    }
}

// Single node display.
impl Display for crate::physical_plan::PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.get_multiline_representation().join(", "))?;
        Ok(())
    }
}

// pub enum DisplayFormatType {
//     Ascii { simple: bool },
//     Tree,
//     Mermaid,
//     Graphviz,
// }

// pub trait DisplayAs {
//     /// Format according to `DisplayFormatType`, used when verbose representation looks
//     fn fmt_as(&self, fmt_type: &DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result;
// }

// impl DisplayAs for crate::LogicalPlan {
//     fn fmt_as(&self, fmt_type: &DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
//         let s = match fmt_type {
//             DisplayFormatType::Ascii { simple } => self.repr_ascii(*simple),
//             DisplayFormatType::Tree => self.repr_indent(),
//             DisplayFormatType::Mermaid => todo!(),
//             DisplayFormatType::Graphviz => todo!(),
//         };
//         write!(f, "{}", s)
//     }
// }
