users = ["fake_user@example.com"]

users.forEach( (user, index) => 
    operate(`cleanup_pii_user_${index}`).dependencies("cleaned").queries(ctx => `
    UPDATE \`whejna-modelling-sandbox.dataform_training.cleaned\` 
    SET author_email = TO_HEX(SHA256(author_email)) 
    WHERE author_email = '${user}';`)
    );

