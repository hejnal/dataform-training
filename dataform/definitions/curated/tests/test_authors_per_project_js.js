const mocked_data = `
SELECT "example" AS type, "test_repo_a" AS sub_repo, (DATE "2023-03-01") AS author_date, "bob@example.com" AS author_email UNION ALL
    SELECT "example" AS type, "test_repo_a" AS sub_repo, (DATE "2023-03-02") AS author_date, "alice@example.com" AS author_email
`;

const expected_data = `
SELECT "example" AS type, "test_repo_a" AS sub_repo, (DATE "2023-03-02") AS last_author_date, ["bob@example.com", "alice@example.com"] AS authors_list, "bob@example.com,alice@example.com" AS authors_str
`

test("test_authors_per_project_js")
  .dataset("authors_per_project")
  .input("cleaned", mocked_data)
  .expect(expected_data);