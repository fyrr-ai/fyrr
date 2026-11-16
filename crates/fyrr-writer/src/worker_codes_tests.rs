use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use syn::parse::Parser;
use syn::visit::{self, Visit};
use syn::{Expr, FnArg, Lit, Pat};

fn test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path()
            .segments
            .last()
            .is_some_and(|s| s.ident == "test")
            || (attr.path().is_ident("cfg")
                && attr
                    .meta
                    .require_list()
                    .is_ok_and(|meta| meta.tokens.to_string() == "test"))
    })
}

#[derive(Default)]
struct Symbols {
    constants: BTreeMap<String, String>,
    arguments: BTreeMap<String, Vec<usize>>,
}
impl<'ast> Visit<'ast> for Symbols {
    fn visit_item_const(&mut self, item: &'ast syn::ItemConst) {
        if let Expr::Lit(expr) = item.expr.as_ref()
            && let Lit::Str(value) = &expr.lit
        {
            self.constants.insert(item.ident.to_string(), value.value());
        }
    }
    fn visit_signature(&mut self, signature: &'ast syn::Signature) {
        if signature.ident == "new" {
            return;
        }
        for (index, arg) in signature.inputs.iter().enumerate() {
            if let FnArg::Typed(arg) = arg
                && let Pat::Ident(name) = arg.pat.as_ref()
                && matches!(name.ident.to_string().as_str(), "code" | "error")
                && let syn::Type::Reference(ty) = arg.ty.as_ref()
                && let syn::Type::Path(ty) = ty.elem.as_ref()
                && ty.path.is_ident("str")
            {
                self.arguments
                    .entry(signature.ident.to_string())
                    .or_default()
                    .push(index);
            }
        }
    }
}

struct Literals<'a> {
    symbols: &'a Symbols,
    codes: &'a mut BTreeSet<String>,
}
impl Literals<'_> {
    fn add(&mut self, value: String) {
        if value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
            && value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            self.codes.insert(value);
        }
    }
}
impl<'ast> Visit<'ast> for Literals<'_> {
    fn visit_lit_str(&mut self, value: &'ast syn::LitStr) {
        self.add(value.value());
    }
    fn visit_expr_path(&mut self, expr: &'ast syn::ExprPath) {
        if let Some(value) = expr
            .path
            .get_ident()
            .and_then(|id| self.symbols.constants.get(&id.to_string()))
        {
            self.add(value.clone());
        }
    }
    fn visit_expr_call(&mut self, expr: &'ast syn::ExprCall) {
        // SCIM's typed protocol errors use a different response channel.
        if let Expr::Path(path) = expr.func.as_ref()
            && path.path.segments.iter().any(|s| s.ident == "ScimError")
        {
            return;
        }
        visit::visit_expr_call(self, expr);
    }
}

struct Errors<'a> {
    symbols: &'a Symbols,
    codes: BTreeSet<String>,
}
impl Errors<'_> {
    fn arguments(&mut self, name: &str, args: impl IntoIterator<Item = Expr>) {
        for (index, expr) in args.into_iter().enumerate() {
            if matches!(name, "Err" | "ok_or" | "ok_or_else" | "map_err")
                || self
                    .symbols
                    .arguments
                    .get(name)
                    .is_some_and(|positions| positions.contains(&index))
            {
                Literals {
                    symbols: self.symbols,
                    codes: &mut self.codes,
                }
                .visit_expr(&expr);
            }
        }
    }
    fn macro_errors(&mut self, tokens: proc_macro2::TokenStream) {
        let mut name = String::new();
        for token in tokens {
            match token {
                proc_macro2::TokenTree::Ident(ident) => name = ident.to_string(),
                proc_macro2::TokenTree::Group(group) => {
                    if let Ok(args) =
                        syn::punctuated::Punctuated::<Expr, syn::Token![,]>::parse_terminated
                            .parse2(group.stream())
                    {
                        self.arguments(&name, args);
                    }
                    self.macro_errors(group.stream());
                    name.clear();
                }
                _ => name.clear(),
            }
        }
    }
}
impl<'ast> Visit<'ast> for Errors<'_> {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if !test_only(&item.attrs) {
            visit::visit_item_mod(self, item);
        }
    }
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if !test_only(&item.attrs) {
            visit::visit_item_fn(self, item);
        }
    }
    fn visit_expr_call(&mut self, expr: &'ast syn::ExprCall) {
        if let Expr::Path(path) = expr.func.as_ref()
            && let Some(name) = path.path.segments.last()
        {
            self.arguments(&name.ident.to_string(), expr.args.iter().cloned());
        }
        visit::visit_expr_call(self, expr);
    }
    fn visit_expr_method_call(&mut self, expr: &'ast syn::ExprMethodCall) {
        self.arguments(&expr.method.to_string(), expr.args.iter().cloned());
        visit::visit_expr_method_call(self, expr);
    }
    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        self.macro_errors(item.tokens.clone());
    }
}

fn source_files(path: &Path, files: &mut Vec<syn::File>) {
    if path
        .file_stem()
        .is_some_and(|name| name == "tests" || name.to_string_lossy().ends_with("_tests"))
    {
        return;
    }
    if path.is_dir() {
        for entry in std::fs::read_dir(path).unwrap() {
            source_files(&entry.unwrap().path(), files);
        }
    } else if path.extension().is_some_and(|e| e == "rs") {
        files.push(syn::parse_file(&std::fs::read_to_string(path).unwrap()).unwrap());
    }
}

#[test]
fn catalog_covers_production_error_expressions() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    source_files(&root.join("src"), &mut files);
    // Writer paths also propagate storage errors and the shared embedding catalog.
    source_files(&root.join("../fyrr-reader/src/storage.rs"), &mut files);
    let mut symbols = Symbols::default();
    for file in &files {
        symbols.visit_file(file);
    }
    let mut errors = Errors {
        symbols: &symbols,
        codes: BTreeSet::new(),
    };
    for file in &files {
        errors.visit_file(file);
    }
    let known: BTreeSet<_> = super::codes().map(str::to_owned).collect();
    let missing: Vec<_> = errors.codes.difference(&known).collect();
    assert!(
        missing.is_empty(),
        "unregistered writer error codes: {missing:?}"
    );
}
