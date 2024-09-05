use lib_data_processing::config::Transform;
use lib_data_processing::schemas::jdd::Jdd;
use lib_data_processing::schemas::AsString;
use lib_data_processing::transforms::{
    col_with_udf_expr, email::col_email_with_polars_expr,
    raison_sociale::col_raison_sociale_with_polars_expr,
};
use criterion::{criterion_group, criterion_main, Criterion};
use polars::prelude::*;

fn benchmark_polars_expr_vs_udf(c: &mut Criterion) {
    // Creating a sample DataFrame for the benchmark
    let df = df![
        Jdd::Email.as_str() => &[
            Some("Lucas31@gmail.com"),
            Some("Lucas 31@gmail.com"),
            Some("Lucàs31@gmail.com"),
            Some("Luc’’as31@gmail.com"),
            Some("Lucas31@gmail.com"),
            Some("@gmail.com"),
            Some("Lucas31gmail.com"),
            Some("Lucas31@g.com"),
            Some("Lucas31@siapartnersrue(XXXX....XXXX).com"),
            Some("Lucas31@"),
            Some("Lucas31@gmail.c-om"),
            Some("Lucas31@.gmail.com"),
            Some("Lucas31@gmail."),
            Some("Lucas31@gmail..com"),
            Some("Lucas31@gmail.f"),
            Some("Lucas31@gmail.commmee"),
            None,
            Some("em&ms@gmail..com")
        ],
         Jdd::RaisonSociale.as_str() => &[
            Some("\"ED\"\"BANGER\""),
            Some("Imagin&tiff_"),
            Some("S’ociété"),
            Some("VECCHIA/"),
            Some("//MONEYY//"),
            Some("Straße"),
            Some("Ve&ccio"),
            Some("édouardservices"),
            Some("imagin//"),
            Some("HecøTOR"),
            Some("ed'GAR"),
            Some("Société dupont"),
            Some("villiers"),
            Some("Paul&JO"),
            Some("\"\"vanescènce\""),
            Some("Brøgger"),
            Some("A"),
            None
            ]

    ]
    .expect("Failed to create DataFrame");

    // Polars expression
    c.bench_function("Polars Expression", |b| {
        b.iter(|| {
            let lf = df.clone().lazy().with_columns(vec![
                col_email_with_polars_expr(lib_data_processing::schemas::SchemasEnum::Jdd),
                col_raison_sociale_with_polars_expr(lib_data_processing::schemas::SchemasEnum::Jdd),
            ]);
            let _ = lf.collect().expect("Failed to collect DataFrame");
        });
    });

    // UDF
    c.bench_function("UDF Expression", |b| {
        b.iter(|| {
            let lf = df.clone().lazy().with_columns(vec![
                col_with_udf_expr(Jdd::Email, Transform::Email),
                col_with_udf_expr(Jdd::RaisonSociale, Transform::RaisonSociale),
            ]);
            let _ = lf.collect().expect("Failed to collect DataFrame");
        });
    });
}

criterion_group!(benches, benchmark_polars_expr_vs_udf);
criterion_main!(benches);
