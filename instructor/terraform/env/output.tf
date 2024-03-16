output "keys" {
#  value = aws_iam_access_key.iam_secret_key[*].encrypted_secret
   value = [for name, user in local.users: {
      u=user,
      i=aws_iam_access_key.iam_secret_key[name].id,
      s=aws_iam_access_key.iam_secret_key[name].encrypted_secret,
   }]

}