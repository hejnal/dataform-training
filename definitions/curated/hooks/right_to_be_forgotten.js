const users = ["fake_user@example.com"]

users.forEach( (user, index) => 
    operate(`right_to_be_forgotten_${index}`).dependencies("cleaned").queries(ctx => `
    DELETE FROM \`whejna-modelling-sandbox.dataform_training.cleaned\` 
    WHERE author_email = '${user}';`)
    );

