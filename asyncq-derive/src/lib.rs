use darling::FromDeriveInput;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Derive the `Job` trait for a struct, registering it as a background job type.
///
/// # Required attributes
///
/// At minimum, specify the queue name:
///
/// ```rust,ignore
/// #[derive(Job, Serialize, Deserialize)]
/// #[job(queue = "emails")]
/// struct WelcomeEmail {
///     email: String,
/// }
/// ```
///
/// # Optional attributes
///
/// | Attribute | Default | Description |
/// |-----------|---------|-------------|
/// | `queue = "name"` | **required** | Queue this job is enqueued to |
/// | `retries = N` | `10` | Max delivery attempts before dead-lettering |
/// | `timeout_secs = N` | none | Per-job execution timeout in seconds |
/// | `kind = "Name"` | struct name | Override the job type identifier |
///
/// # Full example
///
/// ```rust,ignore
/// #[derive(Job, Serialize, Deserialize)]
/// #[job(queue = "payments", retries = 5, timeout_secs = 30)]
/// struct ProcessPayment {
///     order_id: Uuid,
///     amount_cents: u64,
/// }
/// ```
#[proc_macro_derive(Job, attributes(job))]
pub fn derive_job(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match JobInput::from_derive_input(&input) {
        Ok(parsed) => expand(parsed).into(),
        Err(e) => e.write_errors().into(),
    }
}

// ── Attribute parsing ─────────────────────────────────────────────────────────

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(job), supports(struct_any))]
struct JobInput {
    ident: syn::Ident,
    generics: syn::Generics,

    /// Queue name — required.
    queue: String,

    /// Maximum retry attempts. Default: 10.
    #[darling(default = "default_retries")]
    retries: u32,

    /// Optional execution timeout in seconds.
    #[darling(default)]
    timeout_secs: Option<u64>,

    /// Override the job kind string. Defaults to the struct name.
    #[darling(default)]
    kind: Option<String>,
}

fn default_retries() -> u32 {
    10
}

// ── Code generation ───────────────────────────────────────────────────────────

fn expand(input: JobInput) -> proc_macro2::TokenStream {
    let JobInput { ident, generics, queue, retries, timeout_secs, kind, .. } = input;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let kind_str = kind.unwrap_or_else(|| ident.to_string());

    let timeout_expr = match timeout_secs {
        Some(secs) => quote! { ::core::option::Option::Some(#secs) },
        None       => quote! { ::core::option::Option::None },
    };

    quote! {
        #[automatically_derived]
        impl #impl_generics ::asyncq::Job for #ident #ty_generics #where_clause {
            const KIND:         &'static str       = #kind_str;
            const QUEUE:        &'static str       = #queue;
            const MAX_RETRIES:  u32                = #retries;
            const TIMEOUT_SECS: ::core::option::Option<u64> = #timeout_expr;
        }
    }
}
