#use "topfind";;
#thread

#use "biokepi_machine.ml";;

open Biokepi
open KEDSL
open Cmdliner

(* simple aliases *)
let host = Ketrew.EDSL.Host.parse "/tmp/KT-coclomachine/"
let run_program = Machine.run_program biokepi_machine
let (//) = Filename.concat
(* end of aliases *)

(* submit the main task *)
let submit_job 
    fid
    gsurl
    keyfile
    tokenfile
  =
  let master_node =
    let tmpdir = "/tmp/gdc/" in
    let url = 
      sprintf
        "https://gdc-api.nci.nih.gov/data/%s?related_files=true"
        fid
    in
    let gspath = gsurl // fid in
    let auth_header = 
      match tokenfile with
      | Some tf -> sprintf "-H \"X-Auth-Token: $(cat %s)\"" tf
      | None -> ""
    in
    let authcmd =
      match keyfile with
      | Some kf -> 
          sprintf "gcloud auth activate-service-account --key-file=%s" kf
      | None -> "echo 'No auth keyfile provided. Using default auth.'"
    in
    let product =
      let gslscmd = sprintf "gsutil ls %s" gspath in
      let gsls =
        KEDSL.Command.shell
          ~host 
          (sprintf "{ export PATH=/opt/google-cloud-sdk/bin/:$PATH; %s; %s; }" authcmd gslscmd)
      in
      object
        method is_done = Some (`Command_returns (gsls, 0))
      end
    in
    workflow_node
      product
      ~name:("GDC Download: " ^ fid)
      ~make:(run_program
        Program.(
          shf "mkdir -p %s" tmpdir &&
          shf "cd %s" tmpdir &&
          shf "curl -J %s '%s' | tar zx" auth_header url &&
          sh "cat MANIFEST.txt |grep -v '^id' |awk '{ print $3 \" \" $2 }' > CHECKSUM.md5" &&
          sh "md5sum -c CHECKSUM.md5" &&
          sh authcmd &&
          shf "gsutil cp -r %s %s" fid gspath
        )
     )
  in
  Ketrew.Client.submit_workflow master_node

(* Command line options *)
let fid =
  let doc ="File id" in
  Arg.(required & pos 0 (some string) None & info [] ~docv:"GDC_ID" ~doc)

let gsurl =
  let doc="GS URL for the file to be uploaded" in
  Arg.(required & pos 1 (some string) None & info [] ~docv:"GS://URL" ~doc)

let keyfile =
  let doc="Keyfile to be used for gsutil auth" in
  Arg.(value & opt (some string) None & info ["k"; "keyfile"] ~docv:"key.json" ~doc)

let tokenfile =
  let doc="Token file to be used for authentication" in
  Arg.(value & opt (some string) None & info ["t"; "tokenfile"] ~docv:"token.txt" ~doc)

let cmd =
  let doc = "Batch download files from GDC." in
  let version = "0.0.0" in
  let man = [
    `S "Description";
    `P "$(tname) downloads file(s) from GDC";
  ] in
  Term.(const
    submit_job
    $ fid
    $ gsurl
    $ keyfile
    $ tokenfile
  ),
  Term.(info "secotrec-gdc-download" ~version ~doc ~man)

let () = 
  match Cmdliner.Term.eval cmd with 
  | `Error _ -> exit 1
  | _ -> exit 0
(* end of cli options *)
