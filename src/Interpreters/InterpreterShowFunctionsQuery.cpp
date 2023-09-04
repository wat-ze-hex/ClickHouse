#include <Interpreters/InterpreterShowFunctionsQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowFunctionsQuery.h>
#include <Parsers/formatAST.h>

namespace DB
{

InterpreterShowFunctionsQuery::InterpreterShowFunctionsQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}

BlockIO InterpreterShowFunctionsQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), true);
}

String InterpreterShowFunctionsQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowFunctionsQuery &>();

    DatabasePtr db{nullptr};
    if (query.database.empty())
        db = DatabaseCatalog::instance().getSystemDatabase();
    else
        db = DatabaseCatalog::instance().getDatabase(query.database);

    constexpr const char * functions_table = "functions";

    String rewritten_query = fmt::format(
        R"(
SELECT *
FROM {}.{})",
        db->getDatabaseName(),
        functions_table);

    if (!query.like.empty())
    {
        rewritten_query += " WHERE name ";
        rewritten_query += query.case_insensitive_like ? "ILIKE " : "LIKE ";
        rewritten_query += fmt::format("'{}'", query.like);
    }

    return rewritten_query;
}

}
