variable "keybase_user" {
  description = <<-EOM
    Enter the keybase id of a person to encrypt the AWS IAM secret access key.
    Note that you need access to its private key so you can decrypt it. In
    practice that means you specify your own keybase account id.
    To decrypt: `base64 -d | keybase pgp decrypt`
    EOM
  default = "owilleke"
}


resource "aws_iam_user" "workshop_participant" {
  for_each = local.users
  name = lower(each.key)
  path = "/system/"
}

resource "aws_iam_access_key" "iam_secret_key" {
  for_each = local.users
  user = aws_iam_user.workshop_participant[each.key].name
  pgp_key = "keybase:${var.keybase_user}"
}


resource "aws_iam_user_group_membership" "participant_group" {
  for_each = local.users
  user = aws_iam_user.workshop_participant[each.key].name
  groups   = [local.groupname]
}

resource "aws_iam_user_policy" "s3ecr_permissions" {
  for_each = local.users
  name = "PySpark_workshop_Student_S3_access_${each.key}"
  user = each.key
policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowBucketSync",
      "Action": [
        "s3:ListBucket"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::dmacademy-course-assets"
    },
    {
      "Sid": "Stmt1670451800675",
      "Action": [
        "s3:GetObject",
        "s3:ListObject"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::dmacademy-course-assets/pyspark/*"
    }
  ]
}
EOF
}
